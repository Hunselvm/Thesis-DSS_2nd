library(dplyr)
library(data.table)
library(tidyr)
library(lubridate)

#------  Preamble  -------------------------------------------------------------
rm(list=ls())
cat("\014")
setwd("C:/Users/maxva/OneDrive - Tilburg University/Msc. Data Science/Master Thesis/Data")

firm_data <- data.table::fread("firm_characteristics.csv") %>%
  arrange(permno,date) %>%
  mutate(yearmonth = floor_date(date, unit = "month"))

option_data <- data.table::fread("option_data.csv")

#Carr Wu toevoegen!!

linking_table <- data.table::fread("Secid to Permno.csv")

option_data <- merge(option_data, linking_table, by = "secid", all.x = TRUE) %>%
  select(-c(edate, sdate, score)) %>%
  mutate(yearmonth = floor_date(date, unit = "month"))

data.table::setnames(option_data, "PERMNO", "permno")

merged_data <- merge(option_data, firm_data, by = c("permno", "yearmonth"), all.x = TRUE)

merged_data[, c("permno", "yearmonth", "cfadj", "expiry_indicator", "ticker", "date.y") := NULL]

# Define columns to remove
cols_to_remove <- c("secid", "exdate", "last_date",
                    "optionid", "size_grp", "shares", "market_value", "contract_size", "symbol")

setnames(merged_data, "date.x", "date")

merged_data <- merged_data %>%
  mutate(ID = paste(date, symbol, sep = "_"))

# Remove them
strip_data <- copy(merged_data)
strip_data[, (cols_to_remove) := NULL]
strip_data$cp_flag <- ifelse(strip_data$cp_flag == "C", 1, 0)


# Alternative approach using unique() to prevent duplications
new_order <- unique(c("ID", "impl_volatility", names(strip_data)))
setcolorder(strip_data, new_order)

# Filter data to just January 2018 to reduce computation time even further
#strip_data <- strip_data %>% 
  #filter(year(date) == 2018 & month(date) <= 2)

fwrite(strip_data, "full_data_real.csv")
# ====================================================
# Implement the Train-Test Split Using the Custom Sampling Technique
# ====================================================
# Explanation:
# For each day, sort the options by strike price and time to expiry.
# Then assign a sequential row number per day.
# Next, determine the position in each set of 5 observations.
# Finally, designate observations at positions 1, 3, and 5 to the training set and the remaining to the test set.

data_with_positions <- strip_data %>%
  arrange(date, strike_price, time_to_expiry) %>%  # Sort by date, then strike, then maturity
  group_by(date) %>%                               # For each day separately
  mutate(
    row_id            = row_number(),             # Row number within the day
    position_in_group = ((row_id - 1) %% 5) + 1       # Position within group of 5 (values: 1,2,3,4,5)
  ) %>%
  ungroup()

# Create training and test datasets:
train_data <- data_with_positions %>%
  filter(position_in_group %in% c(1, 3, 5))         # 1st, 3rd, and 5th observations (~60% of data)

test_data <- data_with_positions %>%
  filter(!(position_in_group %in% c(1, 3, 5)))      # Remaining observations (~40% of data)

# Optionally, check the proportion within a day
train_prop <- train_data %>% group_by(date) %>% summarise(n = n())
test_prop <- test_data %>% group_by(date) %>% summarise(n = n())
cat("Training set example counts per day:\n")
print(head(train_prop))
cat("Test set example counts per day:\n")
print(head(test_prop))


fwrite(train_data, "train_data_real.csv")
fwrite(test_data, "test_data_real.csv")


