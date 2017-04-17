library(tidyverse)
library(reshape2)

data <- readr::read_csv("../data/master.csv", col_types = "iccccc")

# complaints_by_borough <-
boroughs <-
  data %>%
  select(id, column_name, value) %>%
  filter(column_name == 'Borough')

complaints_by_borough <-
  data %>%
  select(id, column_name, value) %>%
  filter(column_name == 'Complaint Type') %>%
  inner_join(boroughs, by = "id") %>%
  group_by(value.y, value.x) %>%
    summarize(group_size = n()) %>%
    top_n(10) %>%
  ungroup() %>%
  arrange(group_size %>% desc) %>%
  filter(row_number() < 100) %>%
  mutate(
    value.x = stringr::str_to_title(value.x),
    value.y = stringr::str_to_title(value.y)
  ) %>%
  ggplot(mapping = aes(x = value.x, y = group_size)) +
    geom_bar(stat = "identity", width = 0.575) +
    facet_grid(value.y ~ .) +
    scale_y_log10(labels = scales::comma) +
    labs(x = "Complaint type", y = "Number of complaints",
         title = "Top 10 complaints across all boroughs",
         caption = "Plot output of complaints-by-borough.R") +
    theme(
      axis.text.x = element_text(angle = 50, hjust = 1),
      panel.grid.minor.y = element_blank()
    )

complaints_by_borough

ggsave(complaints_by_borough, height = 8, units = "in", 
       device = "png", path = "output",
       filename = "complaints-by-borough.png")
