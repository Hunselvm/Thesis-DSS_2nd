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

# Function to process a single train day and its associated test days
process_train_day <- function(train_day) {
  # Create training set
  train_data <- option_data[date == train_day]
  
  # Skip if training dataset is empty
  if(nrow(train_data) == 0) {
    return(NULL)
  }
  
  # Fit ad hoc Black-Scholes model (quadratic specification)
  ahbs_vol_model <- lm(
    impl_volatility ~ moneyness + I(moneyness^2) +
      time_to_expiry + I(time_to_expiry^2) +
      I(moneyness * time_to_expiry),
    data = train_data
  )
  
  # Calculate BS benchmark volatility (mean of training data)
  bs_vol <- mean(train_data$impl_volatility, na.rm = TRUE)
  
  # Convert train_data to data.table for efficient processing
  train_data <- as.data.table(train_data)
  
  # Predict using both models on training data
  train_data[, iv_ahbs := predict(ahbs_vol_model, newdata = .SD)]
  train_data[, iv_bs := bs_vol]
  
  # Calculate in-sample errors for training data
  train_data[, `:=`(
    iv_ahbs_error = impl_volatility - iv_ahbs,
    iv_bs_error =  impl_volatility - iv_bs,
    prediction_horizon = "training",
    train_date = train_day,
    test_date = train_day
  )]
  
  # Initialize list to hold all results
  results <- list(train = train_data)
  
  # Now process test predictions for this training date
  idx <- which(unique_dates == train_day)
  
  # Process h=1 test day
  if(idx + 1 <= length(unique_dates)) {
    test_day <- unique_dates[idx + 1]
    test_data <- option_data[date == test_day]
    train_day <- unique_dates[idx]
    train_data <- option_data[date == train_day]
    train_data$time_to_expiry <- train_data$time_to_expiry - 1
    
    if(nrow(test_data) > 0) {
      test_data <- as.data.table(test_data)
      
      # Make predictions
      train_data[, iv_ahbs := predict(ahbs_vol_model, newdata = .SD)]
      train_data[, iv_bs := bs_vol]
      
      train_data[, `:=`(
        prediction_horizon = "h=1",
        train_date = train_day,
        test_date = test_day
      )]
      
      results[["h1"]] <- train_data
    }
  }
  
  # Process h=5 test day
  if(idx + 5 <= length(unique_dates)) {
    test_day <- unique_dates[idx + 5]
    test_data <- option_data[date == test_day]
    train_day <- unique_dates[idx]
    train_data <- option_data[date == train_day]
    train_data$time_to_expiry <- train_data$time_to_expiry - 5
    
    if(nrow(test_data) > 0) {
      test_data <- as.data.table(test_data)
      
      # Make predictions
      train_data[, iv_ahbs := predict(ahbs_vol_model, newdata = .SD)]
      train_data[, iv_bs := bs_vol]
      
      train_data[, `:=`(
        prediction_horizon = "h=5",
        train_date = train_day,
        test_date = test_day
      )]
      
      results[["h5"]] <- train_data
    }
  }
  
  # Process h=21 test day
  if(idx + 21 <= length(unique_dates)) {
    test_day <- unique_dates[idx + 21]
    test_data <- option_data[date == test_day]
    train_day <- unique_dates[idx]
    train_data <- option_data[date == train_day]
    train_data$time_to_expiry <- train_data$time_to_expiry - 21
    
    if(nrow(test_data) > 0) {
      test_data <- as.data.table(test_data)
      
      # Make predictions
      train_data[, iv_ahbs := predict(ahbs_vol_model, newdata = .SD)]
      train_data[, iv_bs := bs_vol]
      
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


if(TRUE) {
  # Process all training days
  cat("Processing all training days...\n")
  start_time <- Sys.time()
  
  # Setup parallel processing
  num_cores <- min(detectCores() - 1, 14)
  cat("Using", num_cores, "cores for parallel processing\n")
  
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  # Export necessary data and functions to the cluster
  clusterExport(cl, c("option_data", "unique_dates"))
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
  
  end_time <- Sys.time()
  run_time <- end_time - start_time
  cat("All processing completed in:", format(run_time), "\n")
  
} else {
  cat("Analysis cancelled by user.\n")
}

rm(train_results)

# List of result dataframes and an empty list to store merged outputs
result_names <- c("h1", "h5", "h21")
merged_results <- list()

impl_vol_data <- fread("option_impl_vol_data.csv")

for (res in result_names) {
  result_df <- get(paste0(res, "_results"))  # get h1_results, h5_results, etc.
  
  processed_df <- result_df %>%
    mutate(new_id = paste0(test_date, "_", sub(".*?_(.*)", "\\1", ID))) %>%
    select(-impl_volatility)
  
  merged_df <- processed_df %>%
    left_join(impl_vol_data %>% select(c(impl_volatility, ID)), by = c("new_id" = "ID")) %>%
    na.omit()
  
  # Save each merged dataframe in a list with a name like "h1_merged"
  merged_results[[paste0(res, "_merged")]] <- merged_df
}

# Save complete results
fwrite(train_results, file = "train_results_bs.csv")
fwrite(merged_results[[1]], file = "prediction_results_bs_h1t.csv")
fwrite(merged_results[[2]], file = "prediction_results_bs_h5t.csv")
fwrite(merged_results[[3]], file = "prediction_results_bs_h21t.csv")

