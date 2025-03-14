#load libraries
library(data.table)
library(httr)
library(jsonlite)
library(tidyverse)
library(timetk)
library(pracma)
library(zoo)
library(purrr)
library(broom)
library(dplyr)
library(arrow)
#library(greeks) #load greeks package


#ticker values
tickers <- c("_SPX","_VIX","ARM","QQQ","_RUT","SMCI","BABA","GLD","TLT","BA","MSTR","CRWD","AMD","NVDA","TSLA","NFLX","META","GME")
#

# Loop over each ticker
for (ticker in tickers) {
  print(paste("Processing:", ticker))
# API URL for SPX options data
api_url <- paste0("https://cdn.cboe.com/api/global/delayed_quotes/options/", ticker,".json")
# Fetch the data
response <- GET(api_url)
print(paste("Fetching data for:", ticker))
print(response$status_code)
# Check if the response is successful
if (response$status_code == 200) {
  # Parse the JSON content
  data <- content(response, as = "parsed", type = "application/json")
  # Extract options data
  options_list <- data$data$options
  underlying <- data$data
  underlying$options <- NULL  # Remove the options element
  # Convert the underlying data to a data.table
  underlying <- as.data.table(underlying)
  # Initialize an empty data.table
  options <- data.table()
# Convert the entire list to a data.table directly
  options <- rbindlist(lapply(options_list, as.data.table), fill = TRUE)
  options$ds <- underlying$price_change
  options$pctds <- underlying$price_change_percent/100
  options$S <- underlying$close
  options$openS <- underlying$open
  options$lowS <- underlying$low
  options$highS <- underlying$high
  options$date <- as_date(underlying$last_trade_time)
  options$iv30 <- (underlying$iv30)/100
  cboe_scraper_cleaner <- function(text, ticker) {
    if (ticker == "_SPX") {
      replacements <- list(
        "option" = "option_code",
        "open_interest" = "open_int",
        "000," = ",",
        "C00" = "C",
        "P00" = "P",
        "P0" = "P",
        "C0" = "C",
        "SPX2" = "AM2"
      )
    } else {
      replacements <- list(
        "option" = "option_code",
        "open_interest" = "open_int",
        "000," = ",",
        "C00" = "C",
        "P00" = "P",
        "P0" = "P",
        "C0" = "C",
        "2500" = "2.5",
        "7500" = "7.5",
        "10500" = "10.5",
        "11500" = "11.5",
        "13500" = "13.5",
        "14500" = "14.5",
        "15500" = "15.5",
        "16500" = "16.5",
        "17500" = "17.5",
        "18500" = "18.5",
        "19500" = "19.5",
        "20500" = "20.5"
      )
    }
    for (pattern in names(replacements)) {
      replacement <- replacements[[pattern]]
      text <- gsub(pattern, replacement, text, fixed = TRUE)
    }
    return(text)
  }
#create date labeled file name
file_label <- paste0(as_date(underlying$last_trade_time),ticker,"_options.csv")
# Save the data to a CSV file for further analysis
fwrite(options, file_label, row.names = FALSE)
#read file and clean it
scrape_file <- read_file(file_label)


cleaned_scrape <- cboe_scraper_cleaner(scrape_file,ticker)
# Broken Code Fix
writeLines(cleaned_scrape, paste0("clean", file_label))
###Broken Code#
#fwrite(cleaned_scrape, paste0("clean",file_label))
rm(cleaned_scrape)
#load clean data
options <- fread(paste0("clean",file_label))
#add variables
options <- options %>% 
  select(-bid_size,-ask_size,-tick) %>% 
  mutate(date = ymd(date),
         exp = as.Date(sub("^.*(\\d{6}).*$", "\\1", option_code), format = "%y%m%d"),
         K = as.numeric(sub("^.*[A-Z](\\d+\\.?\\d*)$", "\\1", option_code)),
         isAM = if_else(
           str_detect(option_code,"AM") == TRUE,
           factor("AM"),
           factor("PM")),
         option = if_else(
           str_detect(option_code,"C") == TRUE, 
           factor("call"), 
           factor("put")))
options <- options %>% 
  mutate(option_code = as_factor(option_code),
         mid = if_else(bid == 0, 0.5*(0.025 + ask), 0.5*(bid + ask)),
         bidaskspread = if_else(bid == 0, (ask - 0.025)/ask, (ask - bid)/ask),
         dte = as.numeric(exp - date),
         t = if_else(isAM != "AM",
                     (as.numeric(exp - date)/365),
                     (as.numeric(exp - date)/365) - (6.5/24/365)))
#added columns for parity
options <- options %>% 
  group_by(K,date,isAM,exp) %>%
  mutate(put = if_else(option == "put", mid, NA),
         call = if_else(option == "call", mid, NA),
         .after = ask) %>% 
  fill(c(put, call), #putiv, calliv,last_trade_put,last_trade_call
       .direction = c("downup")) %>%  
  mutate(call_bid = if_else(option == "call", bid, NA),
         call_ask = if_else(option == "call", ask, NA), 
         put_bid = if_else(option == "put", bid, NA),
         put_ask = if_else(option == "put", ask, NA),
         call_volume = if_else(option == "call", volume, NA),
         put_volume = if_else(option == "put", volume, NA)) %>% 
  fill(c(call_bid, call_ask, put_bid, put_ask, call_volume, put_volume),
       .direction = "downup") %>% 
  mutate(p_c = (put - call)
         ) %>% 
  ungroup() %>% 
  rename(prev_close = prev_day_close,
         pct_chg = percent_change,
         last = last_trade_price) %>% 
  relocate(date,t,exp, K, S, mid, put, call, iv, .after = option_code)

#Put-Call Parity for implied interest rate & dividend yield
ols <- options %>%
  ungroup() %>% 
  group_by(t) %>% #beta = implied discount factor
  summarize(beta = sum((K - mean(K, na.rm = TRUE)) * (p_c - mean(p_c, na.rm = TRUE))) / sum((K - mean(K, na.rm = TRUE))^2, na.rm = TRUE),
            alpha = mean(p_c, na.rm = TRUE) - beta * mean(K, na.rm = TRUE)) #S0*exp(-qt)

options <- options %>% left_join(ols, keep = FALSE, by = "t")
rm(ols)
#backing out rates only works for European Options:

#WILL NEED TO USE THE SPX IMPLIED DCF TO USE FOR THE VIX SINCE ITS GETTING 
#STUCK TRYING TO CALCULATE WITH BAD BETAS
if (ticker %in% c("_SPX","_RUT")) {
  #calculate for implied discount factors
  options <- options %>%
    ungroup() %>% 
    group_by(t) %>% 
    mutate(r = -log(beta)/t, #implied interest rate
           f = (-alpha)/beta, #forward equal to S*exp((r-q)*t)
           d = (alpha + S), #discrete dividend
           q = log(alpha/-S)/(-t), #implied dividend yield
           .after = S) %>%
    ungroup() %>% 
    arrange(t,K) %>% 
    mutate(b = r - q, #cost of carry rate
           Sbid = K*exp(-r*t) + call_bid - put_ask,
           Sask = K*exp(-r*t) + call_ask - put_bid,
           logK = log(K/f),
           .after = q)
####  
  #Use library(greeks) for BS implied vol function
  #Run as data.table to efficiently backout Black-Scholes Implied Vol from prices
  df <- data.table(options)
  rm(options)
  #derive bs iv from call prices  
  df[, bsivc := mapply(
    function(option_price, initial_price, exercise_price, time_to_maturity, r, dividend_yield) {
      tryCatch({
        greeks::BS_Implied_Volatility(
          option_price = option_price,
          initial_price = initial_price,
          exercise_price = exercise_price,
          time_to_maturity = time_to_maturity,
          r = r,
          dividend_yield = dividend_yield,
          payoff = "call",
          start_volatility = 0.09
        )
      }, error = function(e) NA)
    },
    option_price = call,
    initial_price = S,
    exercise_price = K,
    time_to_maturity = t,
    r = r,
    dividend_yield = q
  )]
  
  df[, bsivp := mapply(
    function(option_price, initial_price, exercise_price, time_to_maturity, r, dividend_yield) {
      tryCatch({
        greeks::BS_Implied_Volatility(
          option_price = option_price,
          initial_price = initial_price,
          exercise_price = exercise_price,
          time_to_maturity = time_to_maturity,
          r = r,
          dividend_yield = dividend_yield,
          payoff = "put",
          start_volatility = 0.09
        )
      }, error = function(e) NA)
    },
    option_price = put,
    initial_price = S,
    exercise_price = K,
    time_to_maturity = t,
    r = r,
    dividend_yield = q
  )]
  
  #for K < forward use put iv and for K >= forward use call iv
  df <- df %>% 
    mutate(bsiv = if_else(K >= S, bsivc, bsivp),
           .after = S) %>%  
    select(-delta,-gamma,-theta,-vega, -alpha, -beta) #remove unnecessary columns 
  
  
  #calculation of black scholes option greek risk measures
  df <- df %>%
    mutate(z_ = log(K/f) - 0.5*(bsiv^2)*t, #convexity adjusted moneyness 
           z = log(K/f) + 0.5*(bsiv^2)*t, #convexity adjusted moneyness
           .after = f) %>% 
    mutate(d1 = (log(f/K) + ((0.5*bsiv*bsiv)*t))/(bsiv * sqrt(t)), #bms formula
           d2 = d1 - (bsiv * sqrt(t))) %>% 
    mutate(bs_call = exp(-r*t)*(f*pnorm(d1) - K*pnorm(d2)), #black's call
           bs_put = exp(-r*t)*(K*pnorm(-d2) - f*pnorm(-d1)), #black's put
           .after = call) %>%
    mutate(bsdelta = if_else(option == "call", pnorm(d1), pnorm(d1) - 1),
           bsgamma = dnorm(d1)/(f*bsiv*sqrt(t)),
           .after = bs_put)
  #df <- df %>%  mutate(bsgamma=dnorm(d1)/(S*bsiv*sqrt(t))) %>% select(-`dnorm(d1)/(S * bsiv * sqrt(t))`)
  df <- df %>% 
    mutate(bstheta = if_else(option == "call",
                             -exp(-r*t)*(f*bsiv*dnorm(d1)/(2*sqrt(t))) - (r*K*exp(-r*t)*pnorm(d2)) + r*f*exp(-r*t)*pnorm(d1),
                             -exp(-r*t)*(f*bsiv*dnorm(d1)/(2*sqrt(t))) - (r*K*exp(-r*t)*pnorm(-d2)) - r*f*exp(-r*t)*pnorm(-d1)),
           dollar_gamma = (K/(bsiv*sqrt(t)))*dnorm((log(f/K)/(bsiv*sqrt(t))) - bsiv*sqrt(t)*0.5), #black's model
           dollar_vanna = (log(K/f) + 0.5*(bsiv^2)*t)*(f*dnorm(d1)/(bsiv*sqrt(t))),
           dollar_volga = f*dnorm(d1)*(-d1)*(-log(f/K) + 0.5*(bsiv^2)*t),
           .after = dte)
  
  #GVV implied moments extraction
  set.seed(999)
  #GVV method no intercept, can add higher moments
  gvv <- df %>% 
    filter(t > 0, !is.na(bsiv)) %>% 
    nest_by(t) %>% 
    mutate(fitgvv = list(lm(-bstheta ~ I(0.5*dollar_gamma) + 
                              dollar_vanna + I(dollar_volga*0.5) - 1, data = data))) %>% 
    reframe(tidy(fitgvv)) %>% 
    pivot_wider(id_cols = t, names_from = term, values_from = estimate) %>% 
    rename(return_var = `I(0.5 * dollar_gamma)`, #underlying_return_variance
           spotvol_cov = dollar_vanna, #option_volatility_and_underlying_return_covariance
           implvol_var = `I(dollar_volga * 0.5)`) %>%  #variance of the option volatility
    mutate(return_vol = sqrt(abs(return_var)),
           implvol_vol = sqrt(abs(implvol_var)),
           implied_corr = spotvol_cov/(2*return_vol*implvol_vol)) #implied correlation between spot returns and vol returns
  #Join to main table
  df <- df %>%
    left_join(gvv, keep = FALSE, relationship = "many-to-many", by = "t") %>% 
    relocate(return_var:implied_corr, .after = bsiv)
  
  rm(gvv)
  
  df <- df %>% 
    mutate(fitgvv = sqrt(abs(return_var + 2*implied_corr*return_vol*implvol_vol*log(K/f) + implvol_var*(log(K/f)^2))), #extrapolated implied vol
           implied_variance = return_var + 2*implied_corr*return_vol*implvol_vol*log(K/f) + implvol_var*(log(K/f)^2), #extrapolated implied variance
           .after = option_code) %>% 
    mutate(x = (log(K/f) - 0.5*(fitgvv^2)*t)/(fitgvv*sqrt(t)),
           z = (log(K/f) + 0.5*(fitgvv^2)*t),
           z_ = log(K/f) - 0.5*(fitgvv^2)*t) 
  
  df <- df %>% relocate(bsiv,t,K,S,f,r,q:implied_corr,.after = fitgvv)
  
  #At z+ = 0, vanna and volga contributions to PNL -> 0, 
  #here the implied volatility level depends only on the risk-neutral 
  #expected rate of change of the implied volatility
  df <- df %>%
    group_by(t) %>%
    mutate(z0 = if_else(abs(z) == min(abs(z), na.rm = TRUE), K, NA)) %>% 
    mutate(atmgvv = if_else(K == z0, fitgvv, NA), #atmf implied vol gvv fitted
           .after = option_code) %>% 
    fill(atmgvv, z0, .direction = "updown") %>% 
    ungroup() %>% 
    mutate(ivs = fitgvv^2 - atmgvv^2,
           .after = fitgvv)
  
  df <- df %>% 
    mutate(dd1 = (log(f/K) + ((0.5*fitgvv*fitgvv)*t))/(fitgvv * sqrt(t)), #bms formula
           dd2 = d1 - (fitgvv * sqrt(t))) %>% 
    mutate(gvv_call = exp(-r*t)*(f*pnorm(dd1) - K*pnorm(dd2)), #gvv's call
           gvv_put = exp(-r*t)*(K*pnorm(-dd2) - f*pnorm(-dd1)), #gvv's put
           .after = call) %>% 
    mutate(gvvdelta = if_else(option == "call", pnorm(d1), pnorm(d1) - 1),
           gvvgamma = dnorm(d1)/(f*bsiv*sqrt(t)),
           .after = bsgamma) 
  df <- df %>% relocate(call,gvv_call,bs_call,put, gvv_put, bs_put, .after = option_code)
  
  df <- df %>% mutate(part = sub("[CP]\\d+$", "", option_code))
  
  df <- df %>% 
    mutate(option_code = as.character(option_code)) %>% 
    group_by(part)
  
  #save the processed data as a Parquet file
  parquet_file_name <- paste0(as_date(underlying$last_trade_time), ticker, "_options.parquet")
  write_parquet(df, parquet_file_name)
  #save as data set for web application
  if (ticker == "_SPX") {
    df <- df %>% select(-call_volume,-put_volume,-highS,-openS,-lowS)
    write_dataset(df, 
                  partitioning = c("date","part"), 
                  existing_data_behavior = "delete_matching", 
                  path = "path")
    rm(df)
  }else{
    rm(df)
  }
}else{
  #save the processed data as a Parquet file
  parquet_file_name <- paste0(as_date(underlying$last_trade_time), ticker, "_options.parquet")
  write_parquet(options, parquet_file_name)
}


# Clean up objects to free memory
rm(options_list, data, response, scrape_file)
gc()

# Small delay to prevent overloading the server
Sys.sleep(1)

} else {
  # Handle unsuccessful response
  message(paste("Failed to fetch data for ticker:", ticker))
}
}

quit(save = "no")
