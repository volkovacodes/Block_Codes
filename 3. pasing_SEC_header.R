dir_in <- "/Volumes/KINGSTON/Blocks/Clean Forms/"
dir_out <- "/Volumes/KINGSTON/Blocks/Parsed Forms/"
start_year <- 1994
start_QTR <- 1

end_year <- 2018
end_QTR <- 4

require(RSQLite)
require(stringr)
### generate sequence of quaters 
get_dates <- function(start_year, start_QTR, end_year, end_QTR)
{
  require(data.table)
  all_dates <- data.table(year = rep(1993:2050, 4))
  setkey(all_dates,year)
  all_dates[, QTR := 1:.N, by = year]
  all_dates <- as.data.frame(all_dates)
  
  x <- paste0(all_dates$year, all_dates$QTR) >= paste0(start_year, start_QTR) & paste0(all_dates$year, all_dates$QTR) <= paste0(end_year, end_QTR)
  return(all_dates[x,])
}
### parsing SEC header
sec_header <- function(FILENAME, info)
{
  df <- NULL
  df <- data.frame(FILENAME = FILENAME)
  df$CNAME <- str_extract(info, "(?<=COMPANY CONFORMED NAME:\t\t\t).*(?=\n)")
  df$CIK <- str_extract(info, "(?<=INDEX KEY:\t\t\t).*(?=\n)")
  df$SIC <- str_extract(info, "(?<=STANDARD INDUSTRIAL CLASSIFICATION:\t).*(?=\n)")
  df$IRS <- str_extract(info, "(?<=IRS NUMBER:\t\t\t\t).*(?=\n)")
  df$INC_STATE <- str_extract(info, "(?<=STATE OF INCORPORATION:\t\t\t).*(?=\n)")
  df$FYEAR_END <- str_extract(info, "(?<=FISCAL YEAR END:\t\t\t).*(?=\n)")
  # df$FORM_TYPE <- str_extract(info, "(?<=FORM TYPE:\t\t).*(?=\n)")
  ### get business address for the companies that have it
  ### I specify info[business] to distinguish between mail and business address
  business <- grepl("BUSINESS ADDRESS:", info)
  df$business_address_street1[business] <- str_extract(info[business], "(?<=STREET 1:\t\t).*(?=\n)")
  df$business_address_street2[business] <- str_extract(info[business], "(?<=STREET 2:\t\t).*(?=\n)")
  df$business_address_city[business] <- str_extract(info[business], "(?<=CITY:\t\t\t).*(?=\n)")
  df$business_address_state[business] <- str_extract(info[business], "(?<=STATE:\t\t\t).*(?=\n)")
  df$business_address_zip[business] <- str_extract(info[business], "(?<=ZIP:\t\t\t).*(?=\n)")
  df$business_address_phone[business] <- str_extract(info[business], "(?<=BUSINESS PHONE:\t\t).*(?=\n)")
  df$FILENAME <- NULL
  return(df)
}

dates <- get_dates(start_year, start_QTR, end_year, end_QTR)
dates$year_QTR <- paste0(dates$year, dates$QTR)

for(yearqtr in dates$year_QTR)
{
  print(Sys.time())
  print(yearqtr)
  dbname <- paste0(dir_in, "sc13_", yearqtr, ".sqlite")
  con <- dbConnect(drv=RSQLite::SQLite(), dbname=dbname)
  ## Fetch data into data frame
  res <- dbSendQuery(con, "SELECT FILENAME, SBJ, FIL, LINK, DATE, TYPE FROM filings")
  res1 <- dbFetch(res,n=-1)
  
  out <- data.frame(FILENAME = res1$FILENAME, TYPE = res1$TYPE, 
                    DATE = res1$DATE, LINK = res1$LINK)
  ### getting filer informaiton
  df <- sec_header(res1$FILENAME, res1$FIL)
  colnames(df) <- paste0("fil_", colnames(df))
  
  ### getting subject information
  df_sbj <- sec_header(res1$FILENAME, res1$SBJ)
  df_sbj$FILENAME <- NULL
  df_sbj$FORM_TYPE <- NULL
  colnames(df_sbj) <- paste0("sbj_", colnames(df_sbj))
  
  out <- cbind(out, df, df_sbj)
  saveRDS(out, paste0(dir_out, "Parsed_forms_", yearqtr, ".rds"))
}
