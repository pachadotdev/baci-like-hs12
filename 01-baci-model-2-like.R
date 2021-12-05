fout <- "ols_model.rds"

library(arrow)
library(dplyr)
library(tidyr)
library(purrr)
library(cepiigeodist)
library(broom)
library(forcats)
# library(fixest)

source("99-1-clean-funs.R")
source("99-2-model-funs.R")

if (!file.exists(fout)) {
  # Clear and join data ----

  # 1) Select the trade flows for which there is a quantity available, expressed
  # in kg by the importer and the exporter, and for which value is above 10$ and
  # quantity above 2 tonnes.

  # 2) Compute unit values as reported by the exporter (i.e. value reported by the
  # exporter/quantity reported by the exporter) and as reported by the importer
  # (i.e. value reported by the importer/quantity reported by the importer). This
  # yields a ratio of unit values (unit value as reported by the importer/unit
  # value as reported by the exporter) that reflects the CIF/FOB ratio ("CIF
  # rate").

  # This is done by using helper functions

  raw_data <- "raw_dataset_2002_2020"
  prop_data <- "remaining_no_of_rows_after_filtering.rds"

  if (!file.exists(raw_data)) {
    try(dir.create(raw_data))

    dfit <- map(
      # 2002,
      2002:2020,
      function(y) {
        message(y)
        conciliate_flows(y)
      }
    )

    remaining_obs <- map_df(
      seq_along(dfit),
      function(y) {
        tibble(
          year = dfit[[y]][[1]],
          prop_rows_after_filtering = dfit[[y]][[2]]
        )
      }
    )

    dfit <- map_df(seq_along(dfit), function(y) dfit[[y]][[3]])

    dfit <- dfit %>%
      mutate(
        log_cif_fob_unit_ratio = log(cif_fob_unit_ratio),
        log_dist = log(dist),
        log_dist_sq = (log(dist))^2,
        log_uv_exp = log(uv_exp)
      )

    dfit <- dfit %>%
      add_tfe() %>%
      add_pair_id()

    dfit %>%
      group_by(year) %>%
      write_dataset(raw_data)

    saveRDS(remaining_obs, prop_data)
  } else {
    dfit <- arrow::open_dataset(raw_data, partitioning = "year") %>%
      collect() %>%
      mutate(year = as.factor(gsub(".*=", "", year))) %>%
      select(year, everything())

    remaining_obs <- readRDS(prop_data)
  }

  # OLS Model ----

  # 3) Estimate the determinants of this ratio of unit values, by regressing the
  # observed CIF rates on log distance, square of log distance, contiguity,
  # landlocked, median unit value of the product and time fixed effects. Each
  # observation is weighted by the inverse of a "discrepancy ratio" for quantity,
  # i.e. 1/(larger quantity/smaller quantity), the idea being that flows for which
  # both reporters disagree a lot on quantity have less reliable CIF rates.

  # Geographic variables come from the previous version of Gravity (legacy
  # version) and from Geodist. In the next version of BACI the more recent Gravity
  # dataset will be used.

  # After removing influential rows twice, I get a result very similar to model II
  # in the article

  dfit <- dfit %>%
    select(log_cif_fob_unit_ratio, log_dist, log_dist_sq, log_uv_exp, contig,
             landlocked_reporter, landlocked_partner, year, cif_fob_weights)

  form <- log_cif_fob_unit_ratio ~ log_dist + log_dist_sq + log_uv_exp + contig +
    landlocked_reporter + landlocked_partner + year

  fit <- function() {
    lm(
      form,
      data = dfit,
      weights = cif_fob_weights
    )
  }

  fit2 <- fit(); gc()

  # summary(fit2)

  # 4) Based on the results of this estimation, we eliminate the outliers and
  # influential observations. These are the observations for which the studentized
  # residual is above 2; or the Cook's distance is above 4/(n-p-1), where n is the
  # number of observations used in the estimation, and p the number of estimated
  # parameters; or where the leverage is above 2p/n.

  dfit <- remove_outliers(fit2)

  # 5) We reestimate the equation without these outliers. The results of the
  # regression without outliers are used to predict a FOB value for all CIF values
  # reporter by importers.

  fit2 <- fit(); gc()

  dfit <- remove_outliers(fit2)

  fit2 <- fit(); gc()

  dfit <- remove_outliers(fit2)

  fit2 <- fit(); gc()

  rm(dfit)

  model <- list(fit = fit2, outliers = number_outliers(fit2))
  rm(fit2); gc()

  try(dir.create("models"))

  saveRDS(model, fout, compress = "xz")
}
