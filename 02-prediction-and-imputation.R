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

ols_model <- readRDS("~/UN ESCAP/baci-like-hs12/ols_model.rds")

dout <- "imputed_dataset_2002_2020"

rows_with_imputation <- tibble()

map(
  # 2015,
  2002:2020,
  function(y) {
    message(y)
    if (!dir.exists(paste0(dout, "/year=", y))) impute_flows(y)
  }
)

readr::write_csv(rows_with_imputation, "rows_with_imputation_per_year.csv")

# open_dataset(dout, partitioning = "year") %>%
#   filter(year == "year=2015", reporter_iso == "chl", partner_iso == "chn") %>%
#   collect() %>%
#   summarise(trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T))

# OK, close to Aduanas Chile number !
