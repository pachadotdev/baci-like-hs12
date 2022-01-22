library(arrow)
library(dplyr)
library(tidyr)
library(purrr)
library(cepiigeodist)
library(broom)
library(forcats)
# library(fixest)

source("99-1-clean-funs.R")
# source("99-2-model-funs.R")
source("99-3-predict-funs.R")

ols_model <- readRDS("ols_model.rds")
ols_model$fit$model <- NULL
gc()

dout <- "imputed_dataset_2002_2020"

map(
  # 2015,
  2002:2020,
  function(y) {
    message(y)
    if (!dir.exists(paste0(dout, "/year=", y))) impute_flows(y)
  }
)

dexp <- open_dataset("imputed_dataset_2002_2020",
                     partitioning = c("year"))

rows_with_imputation <- tibble()

map(
  2002:2020,
  function(y) {
    rows_with_imputation <<- rows_with_imputation %>%
      bind_rows(
        dexp %>%
          filter(year == paste0("year=", y)) %>%
          select(flag) %>%
          collect() %>%
          group_by(flag) %>%
          count() %>%
          ungroup() %>%
          mutate(m = n / sum(n)) %>%
          mutate(year = y) %>%
          select(year, everything())
      )
  }
)

readr::write_csv(rows_with_imputation, "rows_with_imputation_per_year.csv")

# dexp %>%
#   filter(year == "year=2015", reporter_iso == "chl", partner_iso == "chn") %>%
#   collect() %>%
#   summarise(trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T))

# OK, close to Aduanas Chile number !
