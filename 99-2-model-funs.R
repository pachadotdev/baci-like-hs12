add_tfe <- function(d) {
  d %>%
    mutate(
      etfe = paste(exporter_iso, year, sep = "_"),
      itfe = paste(importer_iso, year, sep = "_")
    ) %>%
    mutate(
      etfe = as.factor(etfe),
      itfe = as.factor(itfe)
    )
}

add_pair_id <- function(d) {
  d %>%
    mutate(
      pair_id = paste(exporter_iso, importer_iso, sep = "_")
    ) %>%
    mutate(
      pair_id = as.factor(pair_id)
    )
}

remove_outliers <- function(fit) {
  n <- nrow(fit$model)
  p <- length(fit$coefficients)

  dfit %>%
    mutate(
      .cooksd = cooks.distance(fit),
      .std.resid = MASS::studres(fit),
      .hat = hatvalues(fit)
    ) %>%
    filter(.std.resid < 2 & .cooksd < 4 / (n - p - 1) & .hat < 2 * p / n)
}

number_outliers <- function(fit) {
  n <- nrow(fit$model)
  p <- length(fit$coefficients)

  augment(fit) %>%
    filter(!(.std.resid < 2 & .cooksd < 4 / (n - p - 1) & .hat < 2*p / n)) %>%
    nrow()
}
