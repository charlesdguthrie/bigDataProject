library(tidyverse)
library(reshape2)

data <- readr::read_csv("master.csv", col_types = "iccccc")

all_complaints <-
  data %>%
  select(column_name, value) %>%
  filter(column_name == 'Complaint Type') %>%
  mutate(
    column_name = as.factor(column_name),
    value = stringr::str_to_title(value)
  ) %>%
  group_by(column_name, value) %>%
    summarize(count = n() / 489607) %>%
  ungroup() %>%
  arrange(count %>% desc) %>%
  filter(row_number() < 25) %>%
  ggplot(mapping = aes(x = reorder(value, count), y = count, fill = count)) +
    geom_bar(stat = "identity", width = 0.575) +
    scale_fill_gradient(low = "beige", high = "firebrick", guide = "none") +
    scale_y_continuous(labels = scales::percent) +
    labs(x = "Complaint type", y = "Number of complaints",
         title = "Top 25 complaints across all boroughs") +
    theme(
      panel.grid.minor.y = element_blank(),
      panel.grid.minor.x = element_blank()
    ) +
    coord_flip()

ggsave(all_complaints, height = 7, units = "in", 
       device = "png", path = "base-type-aggregate", 
       filename = "all-complaints.png")

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
         title = "Top 10 complaints across all boroughs") +
    theme(
      axis.text.x = element_text(angle = 50, hjust = 1),
      panel.grid.minor.y = element_blank()
    )

ggsave(complaints_by_borough, height = 8, units = "in", 
       device = "png", path = "base-type-aggregate", 
       filename = "complaints-by-borough.png")
