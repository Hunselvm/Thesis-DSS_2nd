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

# Compute k and tau (keeping as data.table for performance):
# Here we define relative strike as: k = log(strike_price / stock_price)
train_data[, `:=`(
  k = log(strike_price / stock_price),  # relative strike: ln(K/S)
  tau = time_to_expiry / 365            # time to expiry in years
)]

test_data[, `:=`(
  k = log(strike_price / stock_price),  # relative strike: ln(K/S)
  tau = time_to_expiry / 365            # time to expiry in years
)]

train_data <- train_data %>%
  select(c("ID", "impl_volatility", "k", "tau"))

test_data <- test_data %>%
  select(c("ID", "impl_volatility", "k", "tau"))
# ====================================================
# Carr–Wu Model Calibration and Improved IV Prediction
# ====================================================

# Vectorized implementation of Carr-Wu model for much faster prediction
cw_predict_iv_vectorized <- function(params, tau_vec, k_vec) {
  # Unpack parameters:
  v   <- params[1]
  m   <- params[2]
  w   <- params[3]
  eta <- params[4]
  rho <- params[5]
  
  # Pre-compute common terms for efficiency
  exp_neg_eta_tau <- exp(-eta * tau_vec)
  exp_neg_2eta_tau <- exp(-2 * eta * tau_vec)
  sqrt_v_tau <- sqrt(v * tau_vec)
  
  # Compute quadratic coefficients for X = sigma^2:
  A <- 0.25 * exp_neg_2eta_tau * w^2 * (tau_vec^2)
  B <- 1 - 2 * exp_neg_eta_tau * m * tau_vec - exp_neg_eta_tau * w * rho * sqrt_v_tau
  C <- -(v + 2 * exp_neg_eta_tau * w * rho * sqrt(v) * k_vec + exp_neg_2eta_tau * w^2 * (k_vec^2))
  
  # Calculate the discriminant:
  disc <- B^2 - 4 * A * C
  
  # Initialize result vector
  sigma_pred <- numeric(length(tau_vec))
  
  # For valid cases (A > 0 and disc >= 0), compute solutions
  valid <- (disc >= 0) & (A > 0)
  
  if (any(valid)) {
    # Compute both quadratic solutions where valid:
    sol1 <- (-B + sqrt(disc)) / (2 * A)
    sol2 <- (-B - sqrt(disc)) / (2 * A)
    
    # Choose the positive solutions (prefer the smaller one if both are positive)
    pos1 <- sol1 > 0
    pos2 <- sol2 > 0
    
    # Where both solutions are positive, take the smaller one
    both_pos <- pos1 & pos2 & valid
    sigma_pred[both_pos] <- sqrt(pmin(sol1[both_pos], sol2[both_pos]))
    
    # Where only solution 1 is positive
    only_sol1 <- pos1 & !pos2 & valid
    sigma_pred[only_sol1] <- sqrt(sol1[only_sol1])
    
    # Where only solution 2 is positive
    only_sol2 <- !pos1 & pos2 & valid
    sigma_pred[only_sol2] <- sqrt(sol2[only_sol2])
  }
  
  # Invalid cases remain as 0 in the result
  sigma_pred[!valid] <- NA_real_
  
  return(sigma_pred)
}

# Revised objective function using vectorized prediction
cw_obj_iv_error_vectorized <- function(params, data) {
  # Extract data vectors:
  tau_vec   <- data$tau
  k_vec     <- data$k
  sigma_obs <- data$impl_volatility  # Observed implied volatility
  
  # Compute the predicted implied volatility for all observations at once:
  sigma_pred <- cw_predict_iv_vectorized(params, tau_vec, k_vec)
  
  # Compute the residuals and sum of squared errors
  errors <- sigma_pred - sigma_obs
  sse <- sum(errors^2, na.rm = TRUE)
  
  return(sse)
}

# 3. Calibrate the Carr–Wu Model using the Training Data
# Set an initial parameter guess
init_params <- c(v = 0.04, m = 0.1, w = 0.1, eta = 0.5, rho = -0.5)

# Start profiling for performance analysis
Rprof("profile_optimized.out")

# Use a data.table for the optimization to improve performance
dt_train <- as.data.table(train_data)

# Run the optimization with the vectorized objective function
fit <- optim(
  par    = init_params,
  fn     = cw_obj_iv_error_vectorized,
  data   = dt_train,
  method = "L-BFGS-B",
  lower  = c(v = 1e-6, m = -Inf, w = 1e-6, eta = 1e-6, rho = -1),
  upper  = c(v = Inf,  m = Inf,  w = Inf,  eta = Inf,  rho = 1)
)

# Stop profiling
Rprof(NULL)
summaryRprof("profile_optimized.out")

# Extract the calibrated parameters
theta_hat <- fit$par
cat("Estimated parameters (v, m, w, eta, rho):\n")
print(theta_hat)

# 4. Compute Predicted Implied Volatilities and Errors for Both Sets

# For the training set (used for calibration)
train_preds <- cw_predict_iv_vectorized(theta_hat, train_data$tau, train_data$k)
train_errors <- train_preds - train_data$impl_volatility
train_errors_sq <- train_errors^2

# Assign all results at once - more reliable in data.table
set(train_data, j = "iv_cw", value = train_preds)
set(train_data, j = "iv_cw_error", value = train_errors)
set(train_data, j = "iv_cw_error_sq", value = train_errors_sq)

# For the test set (used to evaluate model performance)
test_preds <- cw_predict_iv_vectorized(theta_hat, test_data$tau, test_data$k)
test_errors <- test_preds - test_data$impl_volatility
test_errors_sq <- test_errors^2

# Assign all results at once - more reliable in data.table
set(test_data, j = "iv_cw", value = test_preds)
set(test_data, j = "iv_cw_error", value = test_errors)
set(test_data, j = "iv_cw_error_sq", value = test_errors_sq)

# 5. Evaluate the Model Performance on the Test Set
rmse_test <- sqrt(mean(test_data$iv_cw_error_sq, na.rm = TRUE))
cat("Carr–Wu RMSE on the test set:", rmse_test, "\n")

# Remove columns from train_data
train_data <- train_data %>% select(-impl_volatility, -k, -tau, -iv_cw_error_sq)

# Remove columns from test_data
test_data <- test_data %>% select(-impl_volatility, -k, -tau, -iv_cw_error_sq)

# Optional: Save results to CSV for further analysis
fwrite(test_data, "cw_test_sameday_real.csv")
fwrite(train_data, "cw_train_sameday_real.csv")

bs_train <- fread("BS_train_sameday_real.csv")
bs_test <- fread("BS_test_sameday_real.csv")

merged_train <- merge(bs_train, train_data, by = "ID")

merged_test <- merge(bs_test, test_data, by = "ID")

new_col_order <- c(
  "ID",
  "train_date", "test_date", 
  "iv_ahbs", "iv_bs", "iv_ahbs_error", "prediction_horizon", "iv_bs_error", "iv_cw", "iv_cw_error"
)

# Define columns to remove
cols_to_remove <- c("ID", "moneyness_category", "date")

# Remove these columns from merged_train
merged_train <- merged_train[, !..cols_to_remove]

# Apply to all three datasets
fwrite(merged_train, "merged_train_sameday_real.csv")
fwrite(merged_test, "merged_test_sameday_real.csv")

