###############################################################################
#################### Setting up ###############################################
###############################################################################
require(pacman)
p_load(data.table, lubridate, dplyr, stringr, Hmisc, httr, zoo, RCurl, DBI, RSQLite)


### folder with Parsed Forms
out_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Parsed Forms/"

start <- 1994
end <- 2021

start_date <- paste(start, "0101") %>% ymd %>% floor_date("quater")
end_date <- paste0(end, "1231") %>% ymd 
dates <- seq(start_date, end_date, by = "quarters")

###############################################################################
#################### Functions  ###############################################
###############################################################################

### parsing SEC header
sec_header <- function(FILENAME, info) {
  require(stringr)
  df <- NULL
  df <- data.frame(FILENAME = FILENAME)
  df$CNAME <- str_extract(info, "(?<=COMPANY CONFORMED NAME:\t\t\t).*(?=\n)")
  df$CIK <- str_extract(info, "(?<=INDEX KEY:\t\t\t).*(?=\n)")
  df$SIC <- str_extract(info, "(?<=STANDARD INDUSTRIAL CLASSIFICATION:\t).*(?=\n)")
  df$IRS <- str_extract(info, "(?<=IRS NUMBER:\t\t\t\t).*(?=\n)")
  df$INC_STATE <- str_extract(info, "(?<=STATE OF INCORPORATION:\t\t\t).*(?=\n)")
  df$FYEAR_END <- str_extract(info, "(?<=FISCAL YEAR END:\t\t\t).*(?=\n)")

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

for (i in 1:length(dates))
{
  print(Sys.time())
  print(paste0("year: ", year(dates[i]), ", quarter: ", quarter(dates[i])))
  dbname <- paste0(out_dir, "Clean Forms/sc13_", year(dates[i]), quarter(dates[i]), ".sqlite")

  require(DBI)
  require(RSQLite)
  con <- dbConnect(drv = RSQLite::SQLite(), dbname = dbname)
  ## Fetch data into data frame
  res <- dbSendQuery(con, "SELECT FILENAME, SBJ, FIL, LINK, DATE, TYPE FROM filings")
  res1 <- dbFetch(res, n = -1)

  out <- data.frame(
    FILENAME = res1$FILENAME, TYPE = res1$TYPE,
    DATE = res1$DATE, LINK = res1$LINK
  )
  ### getting filer informaiton
  df <- sec_header(res1$FILENAME, res1$FIL)
  colnames(df) <- paste0("fil_", colnames(df))

  ### getting subject information
  df_sbj <- sec_header(res1$FILENAME, res1$SBJ)
  df_sbj$FILENAME <- NULL
  df_sbj$FORM_TYPE <- NULL
  colnames(df_sbj) <- paste0("sbj_", colnames(df_sbj))

  out <- cbind(out, df, df_sbj)
  saveRDS(out, paste0(out_dir, "Parsed Forms/", year(dates[i]), quarter(dates[i]), ".rds"))
}
