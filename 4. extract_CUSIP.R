###############################################################################
#################### Setting up ###############################################
###############################################################################
require(pacman)
p_load(data.table, lubridate, dplyr, stringr, Hmisc, httr, zoo, RCurl, DBI, RSQLite)


### folder with Parsed Forms
out_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Parsed Forms/"

start <- 1994
end <- 2021

start_date <- paste(start, "0101") %>% ymd %>% floor_date("quarter")
end_date <- paste0(end, "1231") %>% ymd 
dates <- seq(start_date, end_date, by = "quarters")

###############################################################################
#################### Functions  ###############################################
###############################################################################

### regex to extract CUSIP from filing
extract_CUSIP <- function(EFiling) {
  require(stringr)
  ## get 6 lines before and 4 lines after line CUSIP
  pat_1 <- "((\\n.*){6})CUSIP.*((\\n.*){4})"
  get_block <- str_extract(EFiling, pat_1)

  # set pattern to extract CUSIP
  pat_2 <- "(?=\\d.*\\d)[a-zA-Z0-9]{9}|\\d\\w{6} \\w\\w \\w|\\d\\w{5} \\w\\w \\w|
  [a-zA-Z0-9]{7}\\r|\\d\\w{5} \\w\\w\\w|(?=#.*\\d)[a-zA-Z0-9]{9}|(?=\\w\\d.*)[a-zA-Z0-9]{9}|
  \\d\\w{5}-\\w\\w-\\w|\\d\\w{5}-\\w\\w\\w|\\d\\w{6}|\\d\\w{5}-\\w{2}-\\w|\\d\\w{5}\\n.*\\n.*|
  \\d\\w{2} \\d\\w{2} \\d\\w{2}|\\d\\w{2} \\w{3} \\d{2} \\d|\\d{6} \\d{2} \\d|
  \\d\\w{4} \\w{1} \\w{2} \\w|\\w{6} \\d{2} \\d{1}|\\d{3} \\d{3} \\d{3}|\\d{6} \\d{2} \\d{1}|
  \\w{3} \\w{3} \\d{2} \\d{1}|\\w{5} \\w{1} \\d{2} \\d{1}|\\d{6} \\d{1} \\d{2}|
  \\d{3} \\d{3} \\d{1} \\d{2}|\\d\\w{2}\\n.*\\d\\w{2}|\\d{6} \\d{2}\\n.*|\\d{5} \\d{2} \\d{1}|
  \\d{5} \\w{1} \\w{2} \\w{1}|\\d\\w{5}|\\d\\w{2}-\\w{3}-\\w{3}"


  # Extract CUSIP from within the blocks extracted
  CUSIP <- str_extract(get_block, pat_2)
  return(CUSIP)
}
### remove duplicates from CIK-CUSIP map
CUSIP_table <- function(CUSIP) {
  require(data.table)
  CUSIP_df <- data.table(CUSIP)
  CUSIP_df[, quarter := paste0(year(dates[i]), quarter(dates[i]))]
  CUSIP_df[, CUSIP := gsub("\\s", "", CUSIP)]
  CUSIP_df[, CUSIP := gsub("-", "", CUSIP)]
  CUSIP_df[, CUSIP := toupper(CUSIP)]
  CUSIP_df[, CUSIP6 := substr(CUSIP, 1, 6)]
  CUSIP_df[, CUSIP := substr(CUSIP, 1, 8)]
  return(CUSIP_df)
}

CUSIP_all <- NULL

for (i in 1:length(dates)) {
  print(Sys.time())
  print(paste0("year: ", year(dates[i]), ", quarter: ", quarter(dates[i])))
  dbname <- paste0(out_dir, "Clean Forms/sc13_", year(dates[i]), quarter(dates[i]), ".sqlite")

  ## connect to db
  con <- dbConnect(drv = RSQLite::SQLite(), dbname = dbname)
  ## Fetch data into data frame
  res <- dbSendQuery(con, "SELECT * FROM filings")
  res1 <- dbFetch(res, n = -1)

  sec_name <- paste0(out_dir, "Parsed Forms/", year(dates[i]), quarter(dates[i]), ".rds")
  sec_header <- readRDS(sec_name)
  match <- match(sec_header$FILENAME, res1$FILENAME)
  CUSIP <- extract_CUSIP(res1$FILING)
  CUSIP_df <- CUSIP_table(CUSIP)
  sec_header$CUSIP <- CUSIP_df$CUSIP[match]
  sec_header$CUSIP6 <- CUSIP_df$CUSIP6[match]
  saveRDS(sec_header, sec_name)
  dbDisconnect(con)
}
