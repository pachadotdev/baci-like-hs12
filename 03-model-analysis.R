model_analysis <- list()

# OLS ----

ols_model <- readRDS("ols_model.rds")
model_analysis$ols_reset_pvalue <- lmtest::resettest(ols_model$fit, power = 2)$p.value

# PPML ----

ppml_model <- readRDS("ppml_model.rds")
ppml_model$fit$data$predict2 <- (stats::predict(ppml_model$fit))^2 # Get fitted values of the linear index, not of trade
form_reset <- stats::update(ppml_model$fit$formula, ~ predict2 + .)
fit_reset <- stats::glm(form_reset,
                        family = stats::quasipoisson(link = "log"),
                        data = ppml_model$fit$data,
                        y = FALSE,
                        model = FALSE
)
res <- lmtest::coeftest(fit_reset)
model_analysis$ppml_reset_pvalue <- res[2, 4]

fixest_ppml <- fixest::feglm(
  ppml_model$fit$formula,
  ppml_model$fit$data,
  quasipoisson()
)
model_analysis$ppml_pseudo_rsq <- fixest_ppml$sq.cor

# tables ----

model_analysis$table <- stargazer::stargazer(ols_model$fit, ppml_model$fit)

# negative values ----

model_analysis$ols_negative_fitted <- length(ols_model$fit$fitted.values[ols_model$fit$fitted.values < 0])
model_analysis$ppml_negative_fitted <- length(ppml_model$fit$fitted.values[ppml_model$fit$fitted.values < 1])

saveRDS(model_analysis, "model_analysis.rds")
