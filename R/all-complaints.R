library(tidyverse)
library(reshape2)

data <- readr::read_csv("../data/master.csv", col_types = "iccccc")

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
         title = "Top 25 complaints across all boroughs",
         caption = "Plot output by all-complaints.R") +
    theme(
      panel.grid.minor.y = element_blank(),
      panel.grid.minor.x = element_blank()
    ) +
    coord_flip()

ggsave(all_complaints, height = 7, units = "in",
       device = "png", path = "output",
       filename = "all-complaints.png")
