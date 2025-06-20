library(dplyr)
library(data.table)

#------  Preamble  -------------------------------------------------------------
rm(list=ls())
cat("\014")
setwd("C:/Users/maxva/OneDrive - Tilburg University/Msc. Data Science/Master Thesis/Data")

# 1. Load your data (example uses Yahoo-like columns)
#    Suppose we have a dataset with columns: ticker, date, volume, adj_close
data <- data.table::fread("Option data")

# 2. Calculate average daily *dollar* volume (if that’s your metric)
data_liquidity <- data %>%
  group_by(ticker) %>%
  summarise(avg_daily_dollar_vol = mean(volume, na.rm = TRUE)) %>%
  arrange(desc(avg_daily_dollar_vol))

# 3. Rank and select top 50
top_50 <- data_liquidity %>%
  arrange(desc(avg_daily_dollar_vol)) %>%
  slice(1:50)

top_50$ticker
summary(data)
