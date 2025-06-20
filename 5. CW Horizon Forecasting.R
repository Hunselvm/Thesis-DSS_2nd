# Load necessary libraries
library(dplyr)
library(data.table)
library(tidyr)
library(ggplot2)
library(lubridate)
library(parallel)
library(doParallel)
library(foreach)

# Clear workspace and console
rm(list = ls())
cat("\014")

# Set your working directory
setwd("C:/Users/maxva/OneDrive - Tilburg University/Msc. Data Science/Master Thesis/Data")

# Load the option data
option_data <- data.table::fread("full_data_real.csv")

# Ensure date is in proper format
option_data$date <- as.Date(option_data$date)

# Sort data by date
setorder(option_data, date)

# Get unique dates
unique_dates <- unique(option_data$date)

# Compute k and tau for training data
option_data <- option_data %>%
  mutate(
    k   = log(strike_price / stock_price),
    tau = time_to_expiry / 365
  ) %>% 
  select(ID, impl_volatility, k, tau, date)

# Carr-Wu model functions
cw_predict_iv <- function(params, tau, k) {
  # Unpack parameters:
  v   <- params[1]
  m   <- params[2]
  w   <- params[3]
  eta <- params[4]
  rho <- params[5]
  
  # Compute quadratic coefficients for X = sigma^2:
  A <- 0.25 * exp(-2 * eta * tau) * w^2 * (tau^2)
  B <- 1 - 2 * exp(-eta * tau) * m * tau - exp(-eta * tau) * w * rho * sqrt(v * tau)
  C <- - (v + 2 * exp(-eta * tau) * w * rho * sqrt(v) * k + exp(-2 * eta * tau) * w^2 * (k^2))
  
  # Calculate the discriminant:
  disc <- B^2 - 4 * A * C
  if (disc < 0 || A == 0) return(NA_real_)
  
  # Compute both quadratic solutions:
  sol1 <- (-B + sqrt(disc)) / (2 * A)
  sol2 <- (-B - sqrt(disc)) / (2 * A)
  
  # Choose the positive candidate(s)
  candidates <- c(sol1, sol2)
  pos_candidates <- candidates[candidates > 0]
  if (length(pos_candidates) == 0) return(NA_real_)
  
  # Often the smaller positive solution yields better results:
  X <- min(pos_candidates)
  sigma_pred <- sqrt(X)
  return(sigma_pred)
}

cw_obj_iv_error <- function(params, data) {
  # Extract data vectors:
  tau_vec   <- data$tau
  k_vec     <- data$k
  sigma_obs <- data$impl_volatility
  
  # Compute the predicted implied volatility for each observation using mapply:
  sigma_pred <- mapply(function(tau, k) {
    cw_predict_iv(params, tau, k)
  }, tau_vec, k_vec)
  
  # Compute the residuals (error) directly on the volatility scale:
  errors <- sigma_pred - sigma_obs
  
  # Sum of squared errors
  sse <- sum(errors^2, na.rm = TRUE)
  return(sse)
}

# Function to process a single train day and its associated test days
process_train_day <- function(train_day) {
  # Create training set
  train_data <- option_data[date == train_day]
  
  # Skip if training dataset is empty
  if(nrow(train_data) == 0) {
    return(NULL)
  }
  
  # Set initial parameter guess for Carr-Wu model
  init_params <- c(v = 0.04, m = 0.1, w = 0.1, eta = 0.5, rho = -0.5)
  
  # Calibrate the Carr-Wu model using training data
  fit <- tryCatch({
    optim(
      par    = init_params,
      fn     = cw_obj_iv_error,
      data   = train_data,
      method = "L-BFGS-B",
      lower  = c(v = 1e-6, m = -Inf, w = 1e-6, eta = 1e-6, rho = -1),
      upper  = c(v = Inf,  m = Inf,  w = Inf,  eta = Inf,  rho = 1),
      control = list(maxit = 100)  # Limit max iterations to prevent excessive runtime
    )
  }, error = function(e) {
    # Return NULL if optimization fails
    cat("Optimization failed for train_day:", as.character(train_day), "\n")
    return(NULL)
  })
  
  # If optimization failed, skip this date
  if(is.null(fit)) return(NULL)
  
  # Extract calibrated parameters
  theta_hat <- fit$par
  
  # Convert train_data to data.table for efficient processing
  train_data <- as.data.table(train_data)
  
  # Predict using Carr-Wu model on training data (apply row-wise)
  train_data[, iv_cw := mapply(function(tau, k) {
    cw_predict_iv(theta_hat, tau, k)
  }, tau, k)]
  
  # Calculate in-sample errors for training data
  train_data[, `:=`(
    prediction_horizon = "training",
    train_date = train_day,
    test_date = train_day
  )]
  
  # Initialize list to hold all results
  results <- list(train = train_data)
  
  # Now process test predictions for this training date
  idx <- which(unique_dates == train_day)

  if(idx + 1 <= length(unique_dates)) {
    test_day <- unique_dates[idx + 1]
    test_data <- option_data[date == test_day]
    train_day <- unique_dates[idx]
    train_data <- option_data[date == train_day]
    train_data$tau <- train_data$tau - 1/365
    
    if(nrow(test_data) > 0) {
      test_data <- as.data.table(test_data)
      
      # Make predictions
      train_data[, iv_cw := mapply(function(tau, k) {
        cw_predict_iv(theta_hat, tau, k)
      }, tau, k)]
      
      train_data[, `:=`(
        prediction_horizon = "h=1",
        train_date = train_day,
        test_date = test_day
      )]
      
      results[["h1"]] <- train_data
    }
  }
  
  if(idx + 5 <= length(unique_dates)) {
    test_day <- unique_dates[idx + 5]
    test_data <- option_data[date == test_day]
    train_day <- unique_dates[idx]
    train_data <- option_data[date == train_day]
    train_data$tau <- train_data$tau - 5/365
    
    if(nrow(test_data) > 0) {
      test_data <- as.data.table(test_data)
      
      # Make predictions
      train_data[, iv_cw := mapply(function(tau, k) {
        cw_predict_iv(theta_hat, tau, k)
      }, tau, k)]
      
      train_data[, `:=`(
        prediction_horizon = "h=5",
        train_date = train_day,
        test_date = test_day
      )]
      
      results[["h5"]] <- train_data
    }
  }
  
  if(idx + 21 <= length(unique_dates)) {
    test_day <- unique_dates[idx + 21]
    test_data <- option_data[date == test_day]
    train_day <- unique_dates[idx]
    train_data <- option_data[date == train_day]
    train_data$tau <- train_data$tau - 21/365
    
    # Filter out observations with non-positive tau
    train_data <- train_data[tau > 0]
    
    # Check if we still have data after filtering
    if(nrow(train_data) > 0 && nrow(test_data) > 0) {
      test_data <- as.data.table(test_data)
      
      # Make predictions
      train_data[, iv_cw := mapply(function(tau, k) {
        cw_predict_iv(theta_hat, tau, k)
      }, tau, k)]
      
      train_data[, `:=`(
        prediction_horizon = "h=21",
        train_date = train_day,
        test_date = test_day
      )]
      
      results[["h21"]] <- train_data
    }
  }
  
  return(results)
}

# Create vector of training dates
train_dates <- unique_dates[1:(length(unique_dates)-21)]

# Process all training days
cat("Processing all training days...\n")
start_time <- Sys.time()

# Setup parallel processing
num_cores <- min(detectCores() - 1, 16)
cat("Using", num_cores, "cores for parallel processing\n")

cl <- makeCluster(num_cores)
registerDoParallel(cl)

# Export necessary data and functions to the cluster
clusterExport(cl, c("option_data", "unique_dates", "cw_predict_iv", "cw_obj_iv_error"))
clusterEvalQ(cl, {
  library(data.table)
  library(dplyr)
})

# Process all training days in parallel
all_results <- parLapply(cl, train_dates, process_train_day)

# Stop cluster
stopCluster(cl)

# Filter out NULL results
all_results <- all_results[!sapply(all_results, is.null)]

# Combine all training data
train_results <- rbindlist(lapply(all_results, function(x) x$train))

# Combine test data by horizon
h1_results <- rbindlist(lapply(all_results, function(x) if(!is.null(x$h1)) x$h1 else NULL))
h5_results <- rbindlist(lapply(all_results, function(x) if(!is.null(x$h5)) x$h5 else NULL))
h21_results <- rbindlist(lapply(all_results, function(x) if(!is.null(x$h21)) x$h21 else NULL))

# Combine all test data
all_test_results <- rbindlist(list(h1_results, h5_results, h21_results))

end_time <- Sys.time()
run_time <- end_time - start_time
cat("All processing completed in:", format(run_time), "\n")

# Save complete results
fwrite(train_results, file = "train_results_cw.csv")
fwrite(h1_results, file = "prediction_results_cw_h1.csv")
fwrite(h5_results, file = "prediction_results_cw_h5.csv")
fwrite(h21_results, file = "prediction_results_cw_h21.csv")

train_results  <- fread("train_results_cw.csv")
h1_results     <- fread("prediction_results_cw_h1.csv")
h5_results     <- fread("prediction_results_cw_h5.csv")
h21_results    <- fread("prediction_results_cw_h21.csv")

# Select only ID, iv_cw, iv_cw_error from each dataframe
train_results <- train_results[, .(ID, iv_cw)]
h1_results <- h1_results[, .(ID, iv_cw)]
h5_results <- h5_results[, .(ID, iv_cw)]
h21_results <- h21_results[, .(ID, iv_cw)]

bs_h1 <- fread("prediction_results_bs_h1t.csv")
bs_h5 <- fread("prediction_results_bs_h5t.csv")
bs_h21 <- fread("prediction_results_bs_h21t.csv")
bs_train <- fread("train_results_bs.csv")

# Merge h=1 results
merged_h1 <- merge(h1_results, bs_h1, by = "ID")

# Merge h=5 results
merged_h5 <- merge(h5_results, bs_h5, by = "ID")

# Merge h=21 results
merged_h21 <- merge(h21_results, bs_h21, by = "ID")

#Merge training results
merged_train <- merge(train_results, bs_train, by = "ID")

# Get all column names
names(merged_train)

merged_train <- merged_train %>%
  mutate(iv_cw_error = impl_volatility - iv_cw)

merged_h1 <- merged_h1 %>%
  mutate(iv_bs_error = impl_volatility - iv_bs,
         iv_ahbs_error = impl_volatility - iv_ahbs,
         iv_cw_error = impl_volatility - iv_cw)

merged_h5 <- merged_h5 %>%
  mutate(iv_bs_error = impl_volatility - iv_bs,
         iv_ahbs_error = impl_volatility - iv_ahbs,
         iv_cw_error = impl_volatility - iv_cw)

merged_h21 <- merged_h21 %>%
  mutate(iv_bs_error = impl_volatility - iv_bs,
         iv_ahbs_error = impl_volatility - iv_ahbs,
         iv_cw_error = impl_volatility - iv_cw)


merged_train <- merged_train %>%
  select(iv_bs, iv_bs_error, iv_ahbs, iv_ahbs_error, iv_cw, iv_cw_error, everything()) %>%
  select(-c(ID, test_date, train_date, prediction_horizon, option_price, volume, open_interest))

merged_h1 <- merged_h1 %>%
  select(iv_bs, iv_bs_error, iv_ahbs, iv_ahbs_error, iv_cw, iv_cw_error, everything()) %>%
  select(-c(ID, date.x, date.y, test_date, new_id, train_date, prediction_horizon, option_price, volume, open_interest))

merged_h5 <- merged_h5 %>%
  select(iv_bs, iv_bs_error, iv_ahbs, iv_ahbs_error, iv_cw, iv_cw_error, everything()) %>%
  select(-c(ID, date.x, date.y, test_date, new_id, train_date, prediction_horizon, option_price, volume, open_interest))

merged_h21 <- merged_h21 %>%
  select(iv_bs, iv_bs_error, iv_ahbs, iv_ahbs_error, iv_cw, iv_cw_error, everything()) %>%
  select(-c(ID, date.x, date.y, test_date, new_id, train_date, prediction_horizon, option_price, volume, open_interest))

merged_train <- na.omit(merged_train)
merged_h5 <- na.omit(merged_h5)
merged_h21 <- na.omit(merged_h21)
merged_h1 <- na.omit(merged_h1)

# Save merged datasets 
fwrite(merged_h1, "merged_results_h1.csv")
fwrite(merged_h5, "merged_results_h5.csv")
fwrite(merged_h21, "merged_results_h21.csv")
fwrite(merged_train, "merged_results_train.csv")

 
