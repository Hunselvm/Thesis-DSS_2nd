library(dplyr)
library(data.table)
library(tidyr)
library(zoo)
#------  Preamble  -------------------------------------------------------------
rm(list=ls())
cat("\014")
setwd("C:/Users/maxva/OneDrive - Tilburg University/Msc. Data Science/Master Thesis/Data")

symbols <- fread("symbol_list.csv")

symbols$ID <- paste0(paste0(symbols$V1, " ", symbols$symbol))

# 1. Loading data
data <- data.table::fread("Option data 2017 2023.csv") %>%
  select(impl_volatility, date, symbol)# %>%
  filter(symbol %in% symbols$ID)
  
setDT(data)                # convert (by reference) to data.table  
data[, ID := paste0(date, "_", symbol)]

data <- data[, .(ID, impl_volatility)]

fwrite(data, "option_impl_vol_data.csv", row.names = FALSE)

ids_to_check <- c(
  "2017-02-02_ORCL 170203C39000", "2017-02-02_ORCL 170210C39000",
  "2017-02-02_ORCL 170210C39500", "2017-02-02_ORCL 170217C39000",
  "2017-02-02_ORCL 170217C40000", "2017-02-02_ORCL 170217P36000",
  "2017-02-02_ORCL 170217P38000", "2017-02-02_MSFT 170127C63000",
  "2017-02-02_MSFT 170127C64000", "2017-02-02_MSFT 170127C65000",
  "2017-02-02_MSFT 170127P55500", "2017-02-02_MSFT 170127P57000",
  "2017-02-02_MSFT 170127P58500", "2017-02-02_MSFT 170127P62000",
  "2017-02-02_MSFT 170210P56000", "2017-02-02_MSFT 170210P58000",
  "2017-02-02_MSFT 170217C62500", "2017-02-02_MSFT 170217C65000",
  "2017-02-02_MSFT 170217C67500", "2017-02-02_MSFT 170217C70000",
  "2017-02-02_MSFT 170217P52500", "2017-02-02_MSFT 170217P55000",
  "2017-02-02_MSFT 170217P57500", "2017-02-02_MSFT 170217P60000",
  "2017-02-02_KO 170210C43000",   "2017-02-02_KO 170217C42000",
  "2017-02-02_KO 170217C43000",   "2017-02-02_KO 170217C44000",
  "2017-02-02_KO 170217P38000",   "2017-02-02_KO 170217P39000",
  "2017-02-02_KO 170217P41000",   "2017-02-02_XOM 170127C95000",
  "2017-02-02_XOM 170127P89000",  "2017-02-02_XOM 170127P90000"
)

# 2. Which IDs are present?
present  <- ids_to_check[ids_to_check %in% df$ID]

# 3. Which IDs are missing?
missing  <- setdiff(ids_to_check, df$ID)

# 4. Quick summary
cat("Total to check: ", length(ids_to_check), "\n")
cat(" Present:      ", length(present), "\n")
cat(" Missing:      ", length(missing), "\n\n")

if(length(missing) > 0){
  cat("Missing IDs:\n")
  print(missing)
}