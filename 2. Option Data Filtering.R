library(dplyr)
library(data.table)
library(tidyr)
library(zoo)
library(RColorBrewer)
library(reshape2)  
#------  Preamble  -------------------------------------------------------------
rm(list=ls())
cat("\014")
setwd("C:/Users/maxva/OneDrive - Tilburg University/Msc. Data Science/Master Thesis/Data")

# 1. Loading data
option_data <- data.table::fread("Option data 2017 2023.csv")
stock_data <- data.table::fread("Stock prices 2017 2023.csv")

#Get Secids
unique(stock_data$secid)

# 2. Set variables
stock_data$stock_price <- (stock_data$high + stock_data$low) / 2
st_data <- stock_data[, .(secid, date, ticker, stock_price)]

data <- option_data[st_data, on = .(secid, date), allow.cartesian = TRUE]
rm(option_data, st_data, stock_data)

data$date <- as.Date(data$date)

data$strike_price = data$strike_price/1000
data$moneyness = data$stock_price / (data$strike_price)
data$option_price = (data$best_bid + data$best_offer) / 2

data$time_to_expiry <- as.numeric(as.Date(data$exdate) - as.Date(data$date))



filtered_data <- data %>%
  # Basic filtering: only consider options with reasonable moneyness, volume, and price.
  filter(moneyness >= 0.8 & moneyness <= 1.6,  # widened the upper bound to capture deep OTM puts
         volume >= 100,
         option_price >= 0.125) %>%
  
  # Exclude options that violate put–call parity relationships:
  # For calls, if moneyness > 1 the option is ITM; for puts, if moneyness < 1 the option is ITM.
  filter(!(cp_flag == "C" & moneyness > 1 & option_price > stock_price),
         !(cp_flag == "P" & moneyness < 1 & option_price > stock_price)) %>%
  
  # Exclude options that are mispriced relative to their intrinsic value.
  filter(!(cp_flag == "C" & moneyness < 1 & option_price < (stock_price - strike_price)),
         !(cp_flag == "P" & moneyness > 1 & option_price < (strike_price - stock_price))) %>%
  
  # Remove any options that have unrealistic impl volatility
  filter(impl_volatility >= 0.01 & impl_volatility <= 5) %>%
  
  # **** Key step: Remove ITM options completely ****
  # For calls, keep only those with moneyness < 1; for puts, keep only those with moneyness > 1.
  filter((cp_flag == "C" & moneyness < 1) |
           (cp_flag == "P" & moneyness > 1)) %>%
  
  # Group the remaining options into moneyness categories.
  mutate(
    moneyness_category = case_when(
      cp_flag == "C" & moneyness >= 0.80 & moneyness < 0.90 ~ "DOTMC",  # Deep OTM Call
      cp_flag == "C" & moneyness >= 0.90 & moneyness < 0.97 ~ "OTMC",   # OTM Call
      moneyness >= 0.97 & moneyness < 1.03                   ~ "ATM",    # ATM; note that in this range,
      # only puts are retained since calls become ITM if moneyness is above 1.
      cp_flag == "P" & moneyness >= 1.03 & moneyness < 1.10 ~ "OTMP",   # OTM Put
      cp_flag == "P" & moneyness >= 1.10 & moneyness <= 1.60 ~ "DOTMP",  # Deep OTM Put
      TRUE ~ NA_character_
    )
  ) %>%
  
  # Select the desired columns while dropping unwanted ones.
  select(secid, date, symbol, exdate, last_date, cp_flag,
         option_price, stock_price, moneyness, time_to_expiry,
         everything()) %>%
  select(-i.ticker, -exercise_style, -issuer, -best_offer, -best_bid, -index_flag)

# Examine the filtered dataset
summary(filtered_data)

#interpolate missing risk free rates
risk_free_rate <- data.table::fread("risk-free rate.csv")
risk_free_rate <- risk_free_rate %>%
  rename(date = observation_date) %>%
  arrange(date) %>%
  mutate(DGS10 = na.approx(DGS10, x = date, na.rm = FALSE))

setnames(risk_free_rate, "DGS10", "rf")
option_data <- merge(filtered_data, risk_free_rate, by = "date", all.x = TRUE)
option_data$rf = option_data$rf / 100

symbols_dt <- data.table(symbol = unique(option_data$symbol))

# 2) Write to CSV
fwrite(symbols_dt, "symbol_list.csv")

fwrite(option_data, "option_data.csv", row.names = FALSE)

filtered_data <- filtered_data %>%
  mutate(
    maturity_group = case_when(
      time_to_expiry >= 20 & time_to_expiry <= 60  ~ "Short-term",
      time_to_expiry > 60  & time_to_expiry <= 240 ~ "Long-term",
      TRUE ~ NA_character_
    )
  )

#show obs per day
option_data %>%
  group_by(date) %>%
  summarise(options_per_day = n()) %>%
  summarise(avg_options = mean(options_per_day))

#Create summary table
summary_table <- option_data %>%
  filter(!is.na(moneyness_category)) %>%
  group_by(cp_flag, moneyness_category) %>%
  summarise(
    Price = round(mean(option_price, na.rm = TRUE), 3),
    impl_volatility = round(mean(impl_volatility, na.rm = TRUE), 3),
    Observations = n(),
    .groups = "drop"
  )

library(dplyr)
library(reshape2)
library(lattice)

# Simplify grid by rounding to prevent sparsity
agg_data <- filtered_data %>%
  mutate(
    moneyness_rounded = round(moneyness, 2),
    time_to_expiry_rounded = round(time_to_expiry / 10) * 10
  ) %>%
  group_by(moneyness_rounded, time_to_expiry_rounded) %>%
  summarise(impl_volatility = mean(impl_volatility, na.rm = TRUE), .groups = "drop")

# Convert to matrix
iv_matrix <- acast(agg_data, time_to_expiry_rounded ~ moneyness_rounded, value.var = "impl_volatility")

# Remove completely NA rows and columns to avoid empty plots
iv_matrix <- iv_matrix[rowSums(!is.na(iv_matrix)) > 0, colSums(!is.na(iv_matrix)) > 0]

# Melt back to long format for plotting
df <- melt(iv_matrix, varnames = c("TimeToExpiry", "Moneyness"), value.name = "IV")
df$TimeToExpiry <- as.numeric(as.character(df$TimeToExpiry))
df$Moneyness <- as.numeric(as.character(df$Moneyness))
df <- na.omit(df)

# Plot again
wireframe(
  IV ~ TimeToExpiry * Moneyness,
  data         = df,
  drape        = TRUE,
  colorkey     = FALSE,
  lwd          = 1,
  col.regions  = viridis(100),
  type         = "l",
  scales       = list(arrows = FALSE, cex = 1.5),   # Bigger tick labels
  screen       = list(z = -70, x = -75),
  xlab         = list("Time to Maturity", cex = 1.5, rot = -55),  # Bigger axis label
  ylab         = list("Moneyness", cex = 1.5, rot = 10),
  zlab         = list(label = "Implied Volatility", cex = 1.5, rot = -90),
  par.settings = list(axis.line = list(col = "transparent"))
)

