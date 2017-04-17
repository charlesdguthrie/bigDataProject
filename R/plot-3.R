library(tidyverse)

data <- readr::read_csv("master.csv", col_types = "iccccc")

plot <-
  data %>%
  select(column_name, value) %>%
  filter(column_name == 'Complaint Type') %>%
  mutate(
    column_name = as.factor(column_name)
  ) %>%
  group_by(column_name, value) %>%
    summarize(count = n()) %>%
  ungroup() %>%
  arrange(count %>% desc) %>%
  filter(row_number() < 25) %>%
  ggplot(mapping = aes(x = reorder(value, count), y = count, fill = count)) +
    geom_bar(stat = "identity", width = 0.575) +
    scale_fill_gradient(low = "beige", high = "firebrick", guide = "none") +
    scale_y_continuous(labels = scales::comma) +
    labs(x = "Complaint type", y = "Number of complaints") +
    theme(
      panel.grid.minor.y = element_blank(),
      panel.grid.minor.x = element_blank()
    ) +
    coord_flip()

