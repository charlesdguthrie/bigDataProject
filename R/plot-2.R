library(tidyverse)
library(knitr)

data <- readr::read_csv("master.csv", col_types = "iccccc")

data <- 
  data %>%
  mutate_at(.cols = vars(column_name, base_type, semantic_type, is_valid),
            .funs = funs(as.factor))

div_by_amt_percent <- function(x, amt) {(x / amt) * 100}

data %>%
  group_by(column_name, is_valid) %>%
  summarize(
    distinct_values = n_distinct(value),
    count = n()
  ) %>%
  tidyr::spread(is_valid, count) %>%
  mutate_at(.funs = funs(div_by_amt_percent(., 489607)),
            .cols = vars(invalid, `n/a`, valid)) %>%
  replace_na(replace = list(invalid = 0, `n/a` = 0, valid = 0)) %>%
  kable(digits = 2)

Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

data %>%
  group_by(column_name, is_valid) %>%
  summarize(
    mode = Mode(value)
  ) %>%
  data.frame
