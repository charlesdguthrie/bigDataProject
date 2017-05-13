# Likely have to change the directory.
setwd("~/Documents/courses/spring-2017/big-data/bigDataProject/R/part-2/heat-hypothesis/")

library(tidyverse)
library(stringr)
library(sparklyr)
library(magrittr)
library(ggmap)

complaints <- read_csv(file = "../../../data/311-reduced.csv")
names(complaints) %<>% make.names

only.heat <-
  complaints %>%
  filter(Complaint.Type %>% str_to_lower %>% str_detect(., "heat"))

# heat.line.plot <-
  only.heat %>%
  select(Created.Date, Borough) %>%
  mutate(Borough = Borough %>% as.factor) %>%
  group_by(Borough) %>%
    summarize(count = n()) %>%
    ungroup()
  