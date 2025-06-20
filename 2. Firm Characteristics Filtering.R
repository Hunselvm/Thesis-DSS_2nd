library(dplyr)
library(data.table)
library(tidyr)

#------  Preamble  -------------------------------------------------------------
rm(list=ls())
cat("\014")
setwd("C:/Users/maxva/OneDrive - Tilburg University/Msc. Data Science/Master Thesis/Data")

data <- data.table::fread("Firm Characteristics 2015 2024.csv") %>%
  arrange(permno,date)

data$date = as.Date(data$date)

prop_missing <- data %>%
  sapply(function(x) mean(is.na(x))) %>%
  data.frame() 

na_count <- sapply(data, function(y) sum(length(which(is.na(y)))))
na_count <- data.frame(Variable = names(na_count), NAs = na_count, percent = na_count/nrow(data))

#Remove variables with more than 20% missing, andreou
omit_vars <- subset(na_count, percent > 0.20) #arbitrary choice, 0 because these variables were already removed within var_names_deu

var_names_data <- omit_vars$Variable 

one_value_vars <- sapply(data, function(x) uniqueN(x, na.rm = TRUE) == 1)

data <- data[, !names(data) %in% var_names_data, with = FALSE]
data <- data[, !one_value_vars, with = FALSE]

data[data$me != data$me_company & !is.na(data$me) & !is.na(data$me_company), 
     .(date, permno, me, me_company)][1:10]

#remove unnecessary columns
data <- data %>%
  select(-c(gvkey, prc_high, me_company, 
            ret_exc_lead1m, ret_exc, ret, 
            ret_local, prc_local, prc_low, 
            gics, naics, sic)) %>%
  mutate(permno = as.character(permno))

numeric_cols <- names(data)[sapply(data, is.numeric)]
cols_to_impute <- setdiff(numeric_cols, c("permno", "secid"))

# Impute by date using median
data <- data %>%
  group_by(date) %>%
  mutate(across(all_of(cols_to_impute), ~ ifelse(is.na(.), median(., na.rm = TRUE), .))) %>%
  ungroup()

#Check
na_count2 <- sapply(data, function(y) sum(length(which(is.na(y)))))
na_count2 <- data.frame(Variable = names(na_count2), NAs = na_count2, percent = na_count2/nrow(data))

#update frequency checker
detect_update_frequency <- function(data, id_col = "permno", date_col = "date") {
  # Get numeric columns to check
  numeric_vars <- names(data)[sapply(data, is.numeric)]
  numeric_vars <- setdiff(numeric_vars, c(id_col))
  
  # Ensure date is Date class
  data[[date_col]] <- as.Date(data[[date_col]])
  
  # Extract year for grouping
  data$year <- year(data[[date_col]])
  
  # Prepare result holder
  change_rate <- c()
  
  for (var in numeric_vars) {
    # For each variable, calculate number of unique values per year per firm
    temp <- data %>%
      group_by_at(c(id_col, "year")) %>%
      summarise(n_unique = n_distinct(.data[[var]], na.rm = TRUE), .groups = "drop") %>%
      group_by_at(id_col) %>%
      summarise(avg_n_unique = mean(n_unique, na.rm = TRUE), .groups = "drop")
    
    # Take overall average across firms
    change_rate[var] <- mean(temp$avg_n_unique, na.rm = TRUE)
  }
  
  # Classify
  classification <- sapply(change_rate, function(x) {
    if (x >= 10) "monthly"
    else if (x >= 3) "quarterly"
    else "yearly"
  })
  
  # Return grouped variable names
  list(
    monthly   = names(classification[classification == "monthly"]),
    quarterly = names(classification[classification == "quarterly"]),
    yearly    = names(classification[classification == "yearly"])
  )
}

predictor_frequency <- detect_update_frequency(data)

predictor_columns_monthly   <- predictor_frequency$monthly
predictor_columns_quarterly <- c(predictor_frequency$quarterly, "ni_inc8q", "f_score")
predictor_columns_yearly    <- c("lti_gr1a",     "sale_emp_gr1" , "emp_gr1")

#lag data according to Gu. et al
data_lagged <- data %>%
  arrange(permno, date) %>%
  rename(market_value = market_equity) %>%
  mutate(lag_market_value = lag(market_value, 1L)) %>%
  group_by(permno) %>%
  mutate(across(any_of(predictor_columns_monthly), \(x) lag(x, n = 1))) %>% #We assume Ret are at t, thus t-1 for monthly predictors
  mutate(across(any_of(predictor_columns_quarterly), \(x) lag(x, n = 4))) %>% #lag up to 4 according to Gu. et Al. 2020
  mutate(across(any_of(predictor_columns_yearly), \(x) lag(x, n = 6))) %>% #lag up to 6 according to Gu. et Al. 2020
  slice(-(1:13)) %>%            #To remove missing values in lagged returns up to 12 months
  ungroup() #bidask is missing, calculating manually

data_lagged <- data_lagged %>%
  filter(date >= as.Date("2017-01-01") & date <= as.Date("2023-08-31")) %>%
  select(-c(adjfct, source_crsp, dolvol, tvol))


fwrite(data_lagged, "firm_characteristics.csv")




