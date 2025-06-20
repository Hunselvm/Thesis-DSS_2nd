library(dplyr)
library(data.table)
library(tidyr)

#------  Preamble  -------------------------------------------------------------
rm(list=ls())
cat("\014")
setwd("C:/Users/maxva/OneDrive - Tilburg University/Msc. Data Science/Master Thesis/Data")

filtered_data <- data.table::fread("option_data.csv")

#for summary table
filtered_data <- filtered_data %>%
  mutate(
    moneyness_group = case_when(
      # For CALL options
      cp_flag == "C" & moneyness >= 1.10 & moneyness <= 1.60 ~ "DITM",
      cp_flag == "C" & moneyness >= 1.05 & moneyness < 1.10  ~ "ITM",
      cp_flag == "C" & moneyness >= 1.01 & moneyness < 1.05  ~ "JITM",
      cp_flag == "C" & moneyness >= 0.99 & moneyness < 1.01  ~ "ATM",
      cp_flag == "C" & moneyness >= 0.95 & moneyness < 0.99  ~ "JOTM",
      cp_flag == "C" & moneyness >= 0.90 & moneyness < 0.95  ~ "OTM",
      cp_flag == "C" & moneyness >= 0.80 & moneyness < 0.90  ~ "DOTM",
      
      # For PUT options (reverse scale: K / St = 1 / moneyness)
      cp_flag == "P" & moneyness >= 1.10 & moneyness <= 1.60 ~ "DOTM",
      cp_flag == "P" & moneyness >= 1.05 & moneyness < 1.10  ~ "OTM",
      cp_flag == "P" & moneyness >= 1.01 & moneyness < 1.05  ~ "JOTM",
      cp_flag == "P" & moneyness >= 0.99 & moneyness < 1.01  ~ "ATM",
      cp_flag == "P" & moneyness >= 0.95 & moneyness < 0.99  ~ "JITM",
      cp_flag == "P" & moneyness >= 0.90 & moneyness < 0.95  ~ "ITM",
      cp_flag == "P" & moneyness >= 0.80 & moneyness < 0.90  ~ "DITM",
      
      TRUE ~ NA_character_
    )
  )

filtered_data <- filtered_data %>%
  mutate(
    maturity_group = case_when(
      time_to_expiry >= 20 & time_to_expiry <= 60  ~ "Short-term",
      time_to_expiry > 60  & time_to_expiry <= 240 ~ "Long-term",
      TRUE ~ NA_character_
    )
  )

#show obs per day
filtered_data %>%
  group_by(date) %>%
  summarise(options_per_day = n()) %>%
  summarise(avg_options = mean(options_per_day))

#Create summary table
summary_table <- filtered_data %>%
  filter(!is.na(maturity_group), !is.na(moneyness_group)) %>%
  group_by(cp_flag, maturity_group, moneyness_group) %>%
  summarise(
    Price = round(mean(option_price, na.rm = TRUE), 3),
    Implied_volatility = round(mean(impl_volatility, na.rm = TRUE), 3),
    Observations = n(),
    .groups = "drop"
  )

moneyness_order <- c("DOTM", "OTM", "JOTM", "ATM", "JITM", "ITM", "DITM")

summary_paper <- summary_table %>%
  pivot_longer(cols = c(Price, Implied_volatility, Observations),
               names_to = "row_type", values_to = "value") %>%
  mutate(
    row_type = recode(row_type,
                      "Price" = "Price",
                      "Implied_volatility" = "Implied volatility",
                      "Observations" = "Observations")
  ) %>%
  pivot_wider(
    names_from = moneyness_group,
    values_from = value
  ) %>%
  select(cp_flag, maturity_group, row_type, all_of(moneyness_order)) %>%
  arrange(cp_flag, maturity_group, match(row_type, c("Price", "Implied volatility", "Observations")))
