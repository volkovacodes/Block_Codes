require(RSQLite)
require(stringr)
require(dplyr)

### directory with SQL files
dir_in <- "./Blockholders/SC13_Clean_Filings/"
### directory where to write resuls
dit_out <- "./Blockholders/"
### regex to extract CUSIP from filing
extract_CUSIP <- function(EFiling)
{
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
CUSIP_table <- function(CUSIP)
{
  require(data.table)
  CUSIP_df <- data.table(CUSIP)
  CUSIP_df[, year := ind_year]
  CUSIP_df[, CUSIP := gsub("\\s", "", CUSIP)]
  CUSIP_df[, CUSIP := gsub("-", "", CUSIP)]
  CUSIP_df[, CUSIP := toupper(CUSIP)]
  CUSIP_df[, CUSIP6 := substr(CUSIP,1,6)]
  CUSIP_df[, CUSIP := substr(CUSIP,1,8)]
  CUSIP_df$CIK <- str_extract(res1$SBJ, "(?<=INDEX KEY:\t\t\t).*(?=\n)")
  CUSIP_df$SEC_CIK_Name <- str_extract(res1$SBJ, "(?<=COMPANY CONFORMED NAME:\t\t\t).*(?=\n)")
  CUSIP_df <- CUSIP_df[!duplicated(CUSIP_df)]
  CUSIP_df <- CUSIP_df[!is.na(CUSIP_df$CUSIP6)]
  CUSIP_df <- CUSIP_df[!grepl("0000", CUSIP)]
  setcolorder(CUSIP_df, c("year", "CIK", "SEC_CIK_Name","CUSIP", "CUSIP6"))
  return(CUSIP_df)
}

CUSIP_all <- NULL
for(ind_year in 1995:2015)
{
  dbname <- paste0(dir_in, "sc13_", ind_year, ".sqlite")
  ## connect to db
  con <- dbConnect(drv=RSQLite::SQLite(), dbname=dbname)
  ## Fetch data into data frame
  res <- dbSendQuery(con, "SELECT * FROM filings")
  res1 <- dbFetch(res,n=-1)

  
  CUSIP <- extract_CUSIP(res1$FILING)
  CUSIP_df <- CUSIP_table(CUSIP)
  CUSIP_all <- rbind(CUSIP_all, CUSIP_df)
  dbDisconnect(con)
}

write.csv(CUSIP_all, paste0(dit_out, "CIK_CUSIP.csv"))


