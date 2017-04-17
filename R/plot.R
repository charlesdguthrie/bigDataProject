library(tidyverse)

data <- readr::read_csv("master.csv", col_types = "cfd")

data <- data %>%
  mutate(base_type = as.factor(base_type))

base_type_order <- c('int', 'float', 'datetime', 'string')

plot <-
  data %>%
  complete(nesting(column_name), base_type,
           fill = list(frequency_percentile = 0)) %>%
  ggplot(mapping = aes(x = column_name, y = frequency_percentile)) +
    geom_bar(stat = "identity", position = "dodge",
             alpha = 95/100, width = 0.575) +
    facet_grid(. ~ base_type, 
               labeller = labeller(base_type_order = label_wrap_gen(5))) + 
    labs(x = "Column name", y = "Base type percentile") +
    theme(axis.text.x = element_text(angle = 55, hjust = 1),
          panel.grid.major.x = element_blank(),
          panel.grid.minor.x = element_blank()) +
    coord_flip()

ggsave(plot, height = 7, units = "in", device = "png", path = "base-type-aggregate", filename = "plot.png")
