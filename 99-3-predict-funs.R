impute_flows <- function(y) {
  # read ----

  dexp <- data_partitioned(y) %>%
    filter_flow_impute(y = y, f = "export")

  dreexp <- data_partitioned(y) %>%
    filter_flow_impute(y = y, f = "re-export") %>%
    select(-year)

  dexp <- dexp %>%
    left_join(dreexp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
    substract_re_imp_exp()

  rm(dreexp); gc()

  dimp <- data_partitioned(y) %>%
    filter_flow_impute(y = y, f = "import") %>%
    select(-year)

  dreimp <- data_partitioned(y) %>%
    filter_flow_impute(y = y, f = "re-import") %>%
    select(-year)

  dimp <- dimp %>%
    left_join(dreimp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
    substract_re_imp_exp()

  rm(dreimp); gc()

  dexp <- dexp %>%
    join_flows(dimp)

  rm(dimp); gc()

  beepr::beep(2)

  # convert HS02 to HS12 ----

  if (y < 2012) {
    load("../comtrade-codes/02-2-tidy-product-data/product-correlation.RData")

    hs02_to_hs12 <- product_correlation %>%
      select(hs02, hs12) %>%
      arrange(hs12) %>%
      distinct(hs02, .keep_all = T)

    dexp <- dexp %>%
      left_join(hs02_to_hs12, by = c("commodity_code" = "hs02")) %>%
      select(-commodity_code) %>%
      rename(commodity_code = hs12) %>%
      mutate(
        commodity_code = case_when(
          is.na(commodity_code) ~ "999999",
          TRUE ~ commodity_code
        )
      ) %>%
      group_by(year, reporter_iso, partner_iso, commodity_code, qty_unit_exp, qty_unit_imp) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T),
        qty_exp = sum(qty_exp, na.rm = T),
        qty_imp = sum(qty_imp, na.rm = T),
        netweight_kg_exp = sum(netweight_kg_exp, na.rm = T),
        netweight_kg_imp = sum(netweight_kg_imp, na.rm = T)
      ) %>%
      ungroup()

    beepr::beep(2); beepr::beep(2)
  }

  # impute data ----

  dexp <- dexp %>%
    reported_by()

  dexp <- dexp %>%
    impute_kg() %>%
    compute_uv()

  dexp <- dexp %>%
    add_gravity_cols_left()

  dexp <- dexp %>%
    mutate(
      # log_cif_fob_unit_ratio = log(cif_fob_unit_ratio),
      log_dist = log(dist),
      log_dist_sq = (log(dist))^2,
      log_uv_exp = log(uv_exp)
    ) %>%
    select(year, ends_with("_iso"), commodity_code, reported_by,
           starts_with("trade_"),
           # log_cif_fob_unit_ratio,
           qty_exp, qty_imp, starts_with("qty_unit_"),
           starts_with("netweight_"),
           log_dist, log_dist_sq, log_uv_exp, contig,
           starts_with("landlocked_")
    )

  dexp <- dexp %>%
    left_join(
      readRDS("trade_valuation_system_per_country.rds") %>%
        filter(!is.na(iso3_digit_alpha), year == y) %>%
        select(year, reporter_iso = iso3_digit_alpha, trade_flow, valuation) %>%
        drop_na() %>%
        distinct(year, reporter_iso, trade_flow, .keep_all = T) %>%
        # group_by(year, reporter_iso, trade_flow, valuation) %>%
        # count() %>%
        # filter(n > 1) %>%
        pivot_wider(names_from = trade_flow, values_from = valuation) %>%
        rename(exports_report = Export, imports_report = Import)
    ) %>%
    mutate(year = as.factor(year))

  dexp <- dexp %>%
    mutate(fitted_unit_ratio = predict(ols_model$fit, newdata = dexp)) %>%
    fobization_val() %>%
    fobization_qty_val() %>%
    fobization_system_val() %>%
    impute_trade() %>%
    impute_tag()

  gc()

  dexp <- dexp %>%
    select(year, reporter_iso, partner_iso, commodity_code, trade_value_usd_exp = trade_value_usd_exp_imputed, exports_value)

  dimp <- dexp %>%
    select(year, partner_iso = reporter_iso, reporter_iso = partner_iso, commodity_code, trade_value_usd_imp = trade_value_usd_exp, exports_value)

  dexp <- dexp %>%
    full_join(dimp)

  dexp <- dexp %>%
    select(year, reporter_iso, partner_iso, commodity_code, trade_value_usd_exp, trade_value_usd_imp, flag = exports_value) %>%
    mutate(year = as.integer(as.character(year)))

  dexp <- dexp %>%
    mutate(
      trade_value_usd_exp = ifelse(is.na(trade_value_usd_exp), 0, trade_value_usd_exp),
      trade_value_usd_imp = ifelse(is.na(trade_value_usd_imp), 0, trade_value_usd_imp),
      exchange = trade_value_usd_exp + trade_value_usd_imp
    )

  dexp <- dexp %>%
    filter(exchange > 0) %>%
    select(-exchange)

  unique(dexp$flag)

  rows_with_imputation <<- rows_with_imputation %>%
    bind_rows(
      dexp %>%
        group_by(flag) %>%
        count() %>%
        ungroup() %>%
        mutate(m = n / sum(n)) %>%
        mutate(year = y) %>%
        select(year, everything())
    )

  rm(dimp); gc()

  dexp %>%
    group_by(year) %>%
    write_dataset(dout)

  beepr::beep(2); beepr::beep(2); beepr::beep(2)
}

filter_flow_impute <- function(d, y, f) {
  d %>%
    filter(
      year == y,
      trade_flow == f,
      aggregate_level == 6
    ) %>%
    filter(
      !reporter_iso %in% c("wld", "0-unspecified"),
      !partner_iso %in% c("wld", "0-unspecified")
    ) %>%
    select(year, reporter_iso, partner_iso, commodity_code, trade_value_usd,
           qty_unit, qty, netweight_kg) %>%
    collect()
}

substract_re_imp_exp <- function(d) {
  d %>%
    mutate_if(is.numeric, function(x) ifelse(is.na(x), 0, x)) %>%
    group_by(reporter_iso, partner_iso, commodity_code) %>%
    mutate(
      trade_value_usd = max(trade_value_usd.x - trade_value_usd.y, 0),
      qty = case_when(
        qty_unit.x == qty_unit.y ~ max(qty.x - qty.y, 0),
        TRUE ~ qty.x
      ),
      netweight_kg = max(netweight_kg.x - netweight_kg.y, 0),
      qty_unit = qty_unit.x
    ) %>%
    ungroup() %>%
    select(-ends_with(".x"), -ends_with(".y"))
}

reported_by <- function(d) {
  d %>%
    mutate(
      reported_by = case_when(
        !is.na(trade_value_usd_exp) &
          !is.na(trade_value_usd_imp) ~ "both parties",
        !is.na(trade_value_usd_exp) &
          is.na(trade_value_usd_imp) ~ "exporter only",
        is.na(trade_value_usd_exp) &
          !is.na(trade_value_usd_imp) ~ "importer only"
      ),
      reported_by = as_factor(reported_by)
    )
}

compute_uv <- function(dexp) {
  dexp %>%
    mutate(unit_value_exp = (trade_value_usd_exp / qty_exp)) %>%
    group_by(commodity_code) %>%
    mutate(
      uv_exp = median(unit_value_exp, na.rm = T)
    ) %>%
    ungroup()
}

impute_kg <- function(d) {
  d %>%
    mutate(
      qty_exp = case_when(
        qty_unit_exp == "weight in kilograms" ~ qty_exp,
        qty_unit_exp != "weight in kilograms" ~ netweight_kg_exp
      ),
      qty_imp = case_when(
        qty_unit_imp == "weight in kilograms" ~ qty_imp,
        qty_unit_imp != "weight in kilograms" ~ netweight_kg_imp
      )
    )
}

fobization_val <- function(d) {
  d %>%
    mutate(
      fitted_unit_ratio = exp(fitted_unit_ratio),
      fobization = case_when(
        fitted_unit_ratio >= 1 ~ "fobization",
        fitted_unit_ratio < 1 | is.na(fitted_unit_ratio) | !is.finite(fitted_unit_ratio) ~ "no fobization"
      )
    )
}

fobization_qty_val <- function(d) {
  d %>%
    mutate(
      fobization = case_when(
        is.na(qty_exp) & is.na(qty_imp) ~ "no fobization",
        is.na(qty_unit_exp) & is.na(qty_unit_imp) ~ "no fobization",
        qty_unit_exp == "electrical energy in thousands of kilowatt-hours" |
          qty_unit_imp == "electrical energy in thousands of kilowatt-hours" ~
          "no fobization",
        TRUE ~ fobization
      )
    )
}

fobization_system_val <- function(d) {
  d %>%
    mutate(
      fobization = case_when(
        imports_report != "FOB" ~ "no fobization",
        TRUE ~ fobization
      )
    )
}

impute_trade <- function(d) {
  d %>%
    mutate(trade_value_usd_imp_fob = trade_value_usd_imp / fitted_unit_ratio) %>%

    rowwise() %>%
    mutate(
      direct_mismatch = abs(trade_value_usd_imp - trade_value_usd_exp),
      fobization_mismatch = abs(trade_value_usd_imp_fob - trade_value_usd_exp),
    ) %>%
    ungroup() %>%

    mutate(
      trade_value_usd_exp_imputed = case_when(
        fobization_mismatch < direct_mismatch & fobization == "fobization" ~ trade_value_usd_imp_fob,
        TRUE ~ trade_value_usd_exp
      )
    )
}

impute_tag <- function(d) {
  d %>%
    mutate(
      exports_value = case_when(
        trade_value_usd_exp != trade_value_usd_exp_imputed & fobization == "fobization" ~ "imports, divided by cif/fob ratio",
        TRUE ~ "exports, no change"
      )
    )
}

filter_flow_impute <- function(d, y, f) {
  d %>%
    filter(
      year == y,
      trade_flow == f,
      aggregate_level == 6
    ) %>%
    filter(
      !reporter_iso %in% c("wld", "0-unspecified"),
      !partner_iso %in% c("wld", "0-unspecified")
    ) %>%
    select(year, reporter_iso, partner_iso, commodity_code, trade_value_usd,
           qty_unit, qty, netweight_kg) %>%
    collect()
}

add_valuation <- function(d, y) {
  d %>%
    left_join(
      readRDS("trade_valuation_system_per_country.rds") %>%
        filter(!is.na(iso3_digit_alpha), year == y) %>%
        select(year, reporter_iso = iso3_digit_alpha, trade_flow, valuation) %>%
        drop_na() %>%
        distinct(year, reporter_iso, trade_flow, .keep_all = T) %>%
        # group_by(year, reporter_iso, trade_flow, valuation) %>%
        # count() %>%
        # filter(n > 1) %>%
        pivot_wider(names_from = trade_flow, values_from = valuation) %>%
        rename(exports_report = Export, imports_report = Import)
    )
}

fobization_val <- function(d) {
  d %>%
    mutate(
      fitted_unit_ratio = exp(fitted_unit_ratio),
      fobization = case_when(
        fitted_unit_ratio >= 1 ~ "fobization",
        fitted_unit_ratio < 1 | is.na(fitted_unit_ratio) | !is.finite(fitted_unit_ratio) ~ "no fobization"
      )
    )
}

fobization_qty_val <- function(d) {
  d %>%
    mutate(
      fobization = case_when(
        # is.na(qty_exp) & is.na(qty_imp) ~ "no fobization",
        # is.na(qty_unit_exp) & is.na(qty_unit_imp) ~ "no fobization",
        qty_unit_exp == "electrical energy in thousands of kilowatt-hours" |
          qty_unit_imp == "electrical energy in thousands of kilowatt-hours" ~
          "no fobization",
        TRUE ~ fobization
      )
    )
}

fobization_system_val <- function(d) {
  d %>%
    mutate(
      fobization = case_when(
        imports_report != "CIF" ~ "no fobization",
        TRUE ~ fobization
      )
    )
}
