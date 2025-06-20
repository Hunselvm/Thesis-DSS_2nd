# Load necessary libraries
library(dplyr)
library(data.table)
library(tidyr)
library(ggplot2)

# Clear workspace and console
rm(list = ls())
cat("\014")

# Set your working directory (adjust the path to your own directory)
setwd("C:/Users/maxva/OneDrive - Tilburg University/Msc. Data Science/Master Thesis/Data")

# Load the option data (assumed to be in CSV format)
train_data <- data.table::fread("train_data_real.csv")
test_data <- data.table::fread("test_data_real.csv")


# Fit an ad hoc volatility model using a quadratic specification
ahbs_vol_model <- lm(
  impl_volatility ~ moneyness + I(moneyness^2) +
    time_to_expiry + I(time_to_expiry^2) +
    I(moneyness * time_to_expiry),
  data = train_data
)

# Display a summary of the model
summary(ahbs_vol_model)


train_data <- train_data %>%
  mutate(
    iv_ahbs = predict(ahbs_vol_model, newdata = .)
  )

# Predict implied volatilities using the fitted model
test_data <- test_data %>%
  mutate(
    iv_ahbs = predict(ahbs_vol_model, newdata = .)
  )

bs_vol <- mean(train_data$impl_volatility, na.rm = TRUE)

train_data <- train_data %>%
  mutate(
    iv_ahbs = predict(ahbs_vol_model, newdata = .)
  ) %>% 
  mutate(
    iv_ahbs_error = iv_ahbs - impl_volatility,
    iv_bs         = mean(train_data$impl_volatility, na.rm = TRUE),
    iv_bs_error   = iv_bs - impl_volatility
  )


test_data <- test_data %>%
  mutate(
    iv_ahbs = predict(ahbs_vol_model, newdata = .)
  ) %>% 
  mutate(
    iv_ahbs_error = iv_ahbs - impl_volatility,
    iv_bs         = mean(train_data$impl_volatility, na.rm = TRUE),
    iv_bs_error   = iv_bs - impl_volatility
  )

# Calculate IVRMSE: square root of the mean of the squared errors
ivrmse_ahbs <- sqrt(mean(test_data$iv_ahbs_error**2, na.rm = TRUE))

cat("The Implied Volatility RMSE (IVRMSE) is:", ivrmse_ahbs, "\n")

# Calculate IVRMSE: square root of the mean of the squared errors
ivrmse_bs <- sqrt(mean(test_data$iv_bs_error**2, na.rm = TRUE))

cat("The Implied Volatility RMSE (IVRMSE) is:", ivrmse_bs, "\n")


fwrite(train_data, file = "BS_train_sameday_real.csv")
fwrite(test_data, file = "BS_test_sameday_real.csv")

