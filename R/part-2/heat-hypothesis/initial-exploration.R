setwd("~/Documents/courses/spring-2017/big-data/bigDataProject/R/")

library(tidyverse)
library(stringr)
library(sparklyr)
library(ggmap)

# Setup Spark configuration
# config <- spark_config()
# config$spark.local.dir <- "/Volumes/D128G/tmp"
# sc <- spark_connect(master = "local", config = config, version="2.0.1")

# all.data <- sparklyr::spark_read_csv(sc = sc, name = "data", path = "~/Downloads/311-all.csv",
#                                      header = TRUE, infer_schema = TRUE, memory = FALSE, 
#                                      overwrite = TRUE)

complaints.location <- read_csv(file = "~/Downloads/311-complaint-lat-long.csv")
names(complaints.location) <- complaints.location %>% names %>% make.names

only.heat <-
  complaints.location %>%
  filter(Complaint.Type %>% str_to_lower %>% str_detect(., "heat"))

nyc <- 
  get_map(location = "new york city", color = "bw") %>%
  ggmap +
    theme(
      axis.title = element_blank(),
      axis.ticks = element_blank(),
      axis.text = element_blank(),
      legend.position = "none"
    )

nyc_map <-
  ggmap(nyc, base_layer = ggplot(aes(x = Longitude, y = Latitude), data = only.heat)) +
  stat_density2d(
    aes(fill = ..level.., alpha = ..level..), 
    geom = "polygon", data = only.heat
  ) +
  scale_fill_gradient(low = "gold", high = "red") +
  labs(title = "Heating complaints across NYC") +
  theme(
    axis.title = element_blank(),
    axis.ticks = element_blank(),
    axis.text = element_blank(),
    legend.position = "none"
  )

ggsave(nyc_map, filename = "nyc-heating-map", device = "png",
       height = 8, width = 8, units = "in")

complaint.by.date <- read_csv(file = "../data/311-date-complaint-lat-long.csv")
names(complaint.by.date) <- complaint.by.date %>% names %>% make.names

monthly.heat.complaints <-
  complaint.by.date %>%
  filter(Complaint.Type %>% str_to_lower %>% str_detect(., "heat")) %>%
  mutate(
    month = 
      lubridate::parse_date_time(Created.Date, orders = "mdy HMS") %>% 
      lubridate::month(., label = TRUE, abbr = FALSE) %>%
      as.factor
  )

nyc.monthly.heat <-
  nyc +
    facet_wrap(~ month, ncol = 4) + 
    stat_density2d(
      aes(x = Longitude, y = Latitude, fill = ..level.., alpha = ..level..), 
      geom = "polygon", data = monthly.heat.complaints
    ) +
    scale_fill_gradient(low = "blue", high = "red") +
    labs(title = "Heating complaints across NYC") +
    theme(
      axis.title = element_blank(),
      axis.ticks = element_blank(),
      axis.text = element_blank(),
      legend.position = "none"
    )

ggsave(nyc.monthly.heat, filename = "nyc-monthly-heating.png", device = "png",
       height = 9, width = 9, units = "in")


### 

p <-
  ggmap(nyc, base_layer = ggplot(aes(x = Longitude, y = Latitude), data = monthly.heat.complaints %>% tail(500000))) +
  stat_density2d(
    aes(fill = ..level.., alpha = ..level.., frame = month), 
    geom = "polygon", data = monthly.heat.complaints %>% tail(500000)
  ) +
  scale_fill_gradient(low = "gold", high = "red") +
  labs(title = "Heating complaints across NYC in ") +
  theme(
    axis.title = element_blank(),
    axis.ticks = element_blank(),
    axis.text = element_blank(),
    legend.position = "none"
  )

gganimate(p, filename = "heat-over-time.gif")

complaints.borough.month <- read_csv("../data/311-date-complaint-borough-lat-long.csv")
names(complaints.borough.month) <- complaints.borough.month %>% names %>% make.names

borough.monthly.heat.complaints <-
  complaints.borough.month %>%
  filter(Complaint.Type %>% str_to_lower %>% str_detect(., "heat")) %>%
  mutate(
    month = 
      lubridate::parse_date_time(Created.Date, orders = "mdy HMS") %>% 
      lubridate::month(., label = TRUE, abbr = TRUE) %>%
      as.factor,
    Borough = Borough %>% str_to_title %>% as.factor
  )


# p <-
  borough.monthly.heat.complaints %>%
  group_by(Borough, month) %>%
    summarize(count = n()) %>%
    ungroup() %>%
  ggplot(aes(x = month, y = count, color = Borough, group = Borough)) +
    geom_line() +
    scale_y_continuous(labels = scales::comma) +
    scale_fill_continuous(low = "gold", high = "red", guide = FALSE)

ggsave(p, )

