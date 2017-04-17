library(tidyverse)
library(knitr)

data <- readr::read_csv("master.csv", col_types = "iccccc")

data <- 
  data %>%
  mutate_at(.cols = vars(column_name, base_type, semantic_type, is_valid),
            .funs = funs(as.factor))

div_by_amt_percent <- function(x, amt) {(x / amt) * 100}

# EXCLUDE?
data %>%
  group_by(column_name, value) %>%
    summarize(
      count = n()
    ) %>%
    ungroup() %>%
  arrange(count) %>%
  head(100)
