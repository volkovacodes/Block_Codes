dir_in <- "./Blockholders/SC13_Clean_Filings/"
dir_out <- "./Blockholders/"

require(RSQLite)
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
  df$FORM_TYPE <- str_extract(info, "(?<=FORM TYPE:\t\t).*(?=\n)")
  ### get business address for the companies that have it
  ### I specify info[business] to distinguish between mail and business address
  business <- grepl("BUSINESS ADDRESS:", info)
  df$business_address_street1[business] <- str_extract(info[business], "(?<=STREET 1:\t\t).*(?=\n)")
  df$business_address_street2[business] <- str_extract(info[business], "(?<=STREET 2:\t\t).*(?=\n)")
  df$business_address_city[business] <- str_extract(info[business], "(?<=CITY:\t\t\t).*(?=\n)")
  df$business_address_state[business] <- str_extract(info[business], "(?<=STATE:\t\t\t).*(?=\n)")
  df$business_address_zip[business] <- str_extract(info[business], "(?<=ZIP:\t\t\t).*(?=\n)")
  df$business_address_phone[business] <- str_extract(info[business], "(?<=BUSINESS PHONE:\t\t).*(?=\n)")
  return(df)
}

for(year in 1994:2015)
{
  dbname <- paste0(dir_in, "sc13_", year, ".sqlite")
  con <- dbConnect(drv=RSQLite::SQLite(), dbname=dbname)
  ## Fetch data into data frame
  res <- dbSendQuery(con, "SELECT FILENAME, SBJ, FIL FROM filings")
  res1 <- dbFetch(res,n=-1)
  
  ### getting filer informaiton
  df <- sec_header(res1$FILENAME, res1$FIL)
  colnames(df)[!colnames(df) %in% c("FILENAME", "FORM_TYPE")] <- 
    paste0("fil_", colnames(df)[!colnames(df) %in% c("FILENAME", "FORM_TYPE")])
  
  ### getting subject information
  df_sbj <- sec_header(res1$FILENAME, res1$FIL)
  df_sbj$FILENAME <- NULL
  df_sbj$FORM_TYPE <- NULL
  colnames(df_sbj) <- paste0("sbj_", colnames(df_sbj))
  
  out <- cbind(df, df_sbj)
  write.csv(out, paste0(dir_out, "SEC_header_", year, ".csv"), row.names = F)
}

