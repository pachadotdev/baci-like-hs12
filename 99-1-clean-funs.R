filter_flow <- function(d, y, f) {
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
           qty_unit, qty) %>%
    collect()
}

compute_ratios <- function(dexp) {
  dexp %>%

    # value reported by the exporter / quantity reported by the exporter +
    # value reported by the importer/quantity reported by the importer)
    # This yields a ratio of unit values
    # unit value as reported by the importer/unit value as reported by the
    # exporter) that reflects the CIF/FOB ratio ("CIF rate").
    mutate(
      unit_value_exp = (trade_value_usd_exp / qty_exp),
      unit_value_imp = (trade_value_usd_imp / qty_imp)
    ) %>%

    # Create UV explanatory variable
    # SEE PAGE 14 IN THE ARTICLE
    group_by(commodity_code) %>%
    mutate(
      uv_exp = median(unit_value_exp, na.rm = T)
    ) %>%
    ungroup() %>%

    mutate(
      cif_fob_unit_ratio = unit_value_imp / unit_value_exp,
      cif_fob_ratio = trade_value_usd_imp / trade_value_usd_exp
    ) %>%

    # The unit or net CIF/FOB ratios can be weighted, or not, by the inverse of
    # the gap between reported mirror quantities Min(Qxij,Qmji) / Max(Qxij,Qmji)
    # SEE PAGE 15 IN THE ARTICLE
    rowwise() %>%
    mutate(
      cif_fob_weights = 1 /
        (max(qty_imp, qty_exp, na.rm = T) / min(qty_imp, qty_exp, na.rm = T))
    ) %>%
    ungroup()
}

data_partitioned <- function(y) {
  if (y < 2012) {
    d <- open_dataset("../uncomtrade-datasets-arrow/hs-rev2002/parquet",
                      partitioning = c("aggregate_level", "trade_flow",
                                       "year", "reporter_iso"))
  } else {
    d <- open_dataset("../uncomtrade-datasets-arrow/hs-rev2012/parquet",
                      partitioning = c("aggregate_level", "trade_flow",
                                       "year", "reporter_iso"))
  }

  d %>%
    # need to pass gsub one by one bc arrow translation won't accept the
    # clear_hive function above
    mutate(
      aggregate_level = as.integer(gsub(".*=", "", aggregate_level)),
      trade_flow = gsub(".*=", "", trade_flow),
      year = as.integer(gsub(".*=", "", year)),
      reporter_iso = gsub(".*=", "", reporter_iso)
    )
}

join_flows <- function(dexp, dimp, dreexp, dreimp) {
  dexp %>%
    full_join(dimp, by = c("reporter_iso" = "partner_iso",
                           "partner_iso" = "reporter_iso",
                           "commodity_code")) %>%
    rename(
      trade_value_usd_exp = trade_value_usd.x,
      trade_value_usd_imp = trade_value_usd.y,
      qty_unit_exp = qty_unit.x,
      qty_unit_imp = qty_unit.y,
      qty_exp = qty.x,
      qty_imp = qty.y,
      netweight_kg_exp = netweight_kg.x,
      netweight_kg_imp = netweight_kg.y
    )
}

# ton <- 1016.0469 # kg, UK
# ton <- 907.18474 kg # kg, US
# ton <- 1000 # kg, what BACI uses

filter_kg <- function(dexp, ton = 1000) {
  dexp %>%
    filter(
      qty_unit_exp == "weight in kilograms" &
        qty_unit_imp == "weight in kilograms",
      trade_value_usd_exp > 10 & trade_value_usd_imp > 10,
      qty_exp > 2 * ton & qty_imp > 2 * ton
    )
}

conciliate_flows <- function(y) {
  # read ----
  dexp <- data_partitioned(y) %>%
    filter_flow(y = y, f = "export")

  dimp <- data_partitioned(y) %>%
    filter_flow(y = y, f = "import") %>%
    select(-year)

  # join mirrored flows ----

  # we do a full join, so all trade flows reported either by the importer or the
  # exporter are present in our intermediary datasets
  dexp <- join_flows(dexp, dimp)
  rm(dimp); gc()

  # work on obs. with kilograms ----

  # NO NEED TO RUN THIS COMMENTED SECTION, IT WAS RUN ONCE TO EXTRACT THE UNIQUE
  # CASES
  # units <- purrr::map_df(
  #   1988:2019,
  #   function(t) {
  #     open_dataset("../uncomtrade-datasets-arrow/hs-rev1992/parquet",
  #       partitioning = c("year", "trade_flow", "reporter_iso")) %>%
  #       filter(
  #         year == t,
  #         trade_flow == "import",
  #         aggregate_level == 4) %>%
  #       select(qty_unit) %>%
  #       collect() %>%
  #       distinct()
  #   }
  # )
  #
  # units <- units %>% distinct() %>% pull()
  #
  # [1]  "no quantity"
  # "number of items"
  # [3]  "number of pairs"
  # "area in square metres"
  # [5]  "weight in kilograms"
  # "length in metres"
  # [7]  "volume in litres"
  # "electrical energy in thousands of kilowatt-hours"
  # [9]  "weight in carats"
  # "volume in cubic meters"
  # [11] "thousands of items"
  # "dozen of items"
  # [13] "number of packages"

  # 1) Select the trade flows for which there is a quantity available,
  # expressed in kg by the importer and the exporter, and for which value is
  # above 10$ and quantity above 2 tonnes.

  n <- nrow(dexp)

  dexp <- filter_kg(dexp)

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
      group_by(year, reporter_iso, partner_iso, commodity_code) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T),
        qty_exp = sum(qty_exp, na.rm = T),
        qty_imp = sum(qty_imp, na.rm = T)
      ) %>%
      ungroup()
  }

  # obtain unit values ----

  # 2) Compute unit values as reported by the exporter
  # See the comments in the helper function

  dexp <- dexp %>%
    compute_ratios() %>%
    select(year, reporter_iso, partner_iso, commodity_code,
           starts_with("uv_"), starts_with("cif_fob_")) %>%
    mutate(year = as_factor(year))

  unique_pairs <- dexp %>%
    select(reporter_iso, partner_iso, commodity_code) %>%
    distinct() %>%
    nrow()

  stopifnot(unique_pairs == nrow(dexp))

  p <- nrow(dexp) / n
  message(paste("Proportion of filtered rows / total rows", p))

  return(list(
    y,
    p,
    dexp %>%
      add_gravity_cols() %>%
      add_rta_col(y) %>%
      mutate(
        contig = as.integer(contig),
        comlang_off = as.integer(comlang_off),
        colony = as.integer(colony),
        rta = as.integer(rta)
      ) %>%
      rename(
        exporter_iso = reporter_iso,
        importer_iso = partner_iso
      )
  ))
}

fix_iso_codes <- function(val) {
  case_when(
    val == "rom" ~ "rou", # Romania
    val == "yug" ~ "scg", # Just for joins purposes, Yugoslavia splitted for the
    # analyzed period
    val == "tmp" ~ "tls", # East Timor
    val == "zar" ~ "cod", # Congo (Democratic Republic of the)
    TRUE ~ val
  )
}

add_gravity_cols <- function(d) {
  # Geographic variables come from the previous version of Gravity (legacy
  # version) and from Geodist. In the next version of BACI the more recent
  # Gravity dataset will be used.

  d %>%
    inner_join(
      dist_cepii %>%
        select(reporter_iso = iso_o, partner_iso = iso_d, dist, contig,
               colony, comlang_off) %>%
        mutate_if(is.character, tolower) %>%
        mutate(
          reporter_iso = fix_iso_codes(reporter_iso),
          partner_iso = fix_iso_codes(partner_iso)
        )
    ) %>%
    inner_join(
      geo_cepii %>%
        mutate_if(is.character, tolower) %>%
        select(reporter_iso = iso3, landlocked_reporter = landlocked) %>%
        mutate(reporter_iso = fix_iso_codes(reporter_iso)) %>%
        distinct()
    ) %>%
    inner_join(
      geo_cepii %>%
        mutate_if(is.character, tolower) %>%
        select(partner_iso = iso3, landlocked_partner = landlocked) %>%
        mutate(partner_iso = fix_iso_codes(partner_iso)) %>%
        distinct()
    ) %>%
    mutate_if(is.character, as_factor)
}

add_gravity_cols_left <- function(d) {
  # Geographic variables come from the previous version of Gravity (legacy
  # version) and from Geodist. In the next version of BACI the more recent
  # Gravity dataset will be used.

  d %>%
    left_join(
      dist_cepii %>%
        select(reporter_iso = iso_o, partner_iso = iso_d, dist, contig,
               colony, comlang_off) %>%
        mutate_if(is.character, tolower) %>%
        mutate(
          reporter_iso = fix_iso_codes(reporter_iso),
          partner_iso = fix_iso_codes(partner_iso)
        )
    ) %>%
    left_join(
      geo_cepii %>%
        mutate_if(is.character, tolower) %>%
        select(reporter_iso = iso3, landlocked_reporter = landlocked) %>%
        mutate(reporter_iso = fix_iso_codes(reporter_iso)) %>%
        distinct()
    ) %>%
    left_join(
      geo_cepii %>%
        mutate_if(is.character, tolower) %>%
        select(partner_iso = iso3, landlocked_partner = landlocked) %>%
        mutate(partner_iso = fix_iso_codes(partner_iso)) %>%
        distinct()
    ) %>%
    mutate_if(is.character, as_factor)
}

add_rta_col <- function(d,y) {
  y2 <- paste0("year=", y)
  rtas <- open_dataset("../rtas-and-tariffs/rtas", partitioning = "year") %>%
    filter(year == y2) %>%
    collect() %>%
    mutate(
      rta = as.integer(rta),
      year = as.integer(sub("year=", "", year))
    )

  d2 <- d %>%
    select(year, reporter_iso, partner_iso) %>%
    mutate_if(is.factor, as.character)


  d2 <- d2 %>%
    rowwise() %>%
    mutate(
      country1 = min(reporter_iso, partner_iso),
      country2 = max(reporter_iso, partner_iso)
    ) %>%
    ungroup() %>%
    select(country1, country2) %>%
    left_join(
      rtas %>%
        select(country1, country2, rta)
    ) %>%
    mutate(rta = ifelse(is.na(rta), 0, rta))

  d <- d %>%
    bind_cols(d2 %>% select(rta))

  return(d)
}
