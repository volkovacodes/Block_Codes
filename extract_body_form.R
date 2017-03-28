start_year <- 2014
start_QTR <- 1

end_year <- 2015
end_QTR <- 4
wd <- "/Volumes/ORHAHOG_USB/Blocks/"
setwd(wd)
require(DBI)
require(RSQLite)
require(XML)
require(data.table)
require(stringr)
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

### this function is taken from iangow github
html2txt <- function(file) {
  library(XML)
  xpathApply(htmlParse(file, encoding="UTF-8"), "//body", xmlValue)[[1]] 
}
### this function extracts text from txt/htm 
### of the first filings file in the sequence
get_body <- function(webpage){
  webpage <- unlist(strsplit(webpage, "\n"))
  file.name <- gsub("<FILENAME>","",grep("<FILENAME>.*$", webpage,  perl=TRUE, value=TRUE))
  start.line <- grep("<DOCUMENT>.*$", webpage,  perl=TRUE)
  end.line <- grep("</DOCUMENT>.*$", webpage,  perl=TRUE) 
  if(length(start.line)*length(end.line) == 0) return(NA)
  if(length(file.name) == 0) return(paste0(webpage[start.line:end.line], collapse = "\n"))
  file.ext <- tolower(gsub(".*\\.(.*?)$", "\\1", file.name[1]))

  start.line <- start.line[1]
  end.line <- end.line[1]
  if (file.ext %in% c("htm", "xls", "xlsx", "js", "css", "paper", "xsd")) 
  {
    temp <- webpage[start.line:end.line]
    pdf.start <- grep("^<TEXT>", temp,  perl=TRUE) +1
    pdf.end <- grep("^</TEXT>", temp,  perl=TRUE) -1  
    res <- try(text <- html2txt(temp[pdf.start:pdf.end]),silent = T)
    if(class(res) == "try-error") text <- temp[pdf.start:pdf.end]
    
  }
  if (file.ext=="txt") 
  {
    text <- webpage[start.line:end.line]
    text <- paste0(text, collapse = "\n")
  }
  return(text)
}
### here I extract sbj info
get_sbj <- function(webpage){
  webpage <- unlist(strsplit(webpage, "\n"))
  sbj.line <- grep("SUBJECT COMPANY:", webpage)
  fil.line <- grep("FILED BY:", webpage)
  start.line <- grep("<DOCUMENT>.*$", webpage,  perl=TRUE) 
  if(length(sbj.line)*length(fil.line)*length(start.line) == 0) return(NA)
  start.line <- start.line[1]
  if(sbj.line < fil.line)
  {
    sbj_info <- webpage[sbj.line:fil.line]
  }
  if(fil.line < sbj.line)
  {
    sbj_info <- webpage[sbj.line:start.line]
  }
  
  sbj_info <- paste0(sbj_info, collapse = "\n")
  return(sbj_info)
}
### here I extract fil info
get_fil <- function(webpage){
  webpage <- unlist(strsplit(webpage, "\n"))
  sbj.line <- grep("SUBJECT COMPANY:", webpage)
  fil.line <- grep("FILED BY:", webpage)
  start.line <- grep("<DOCUMENT>.*$", webpage,  perl=TRUE) 
  if(length(sbj.line)*length(fil.line)*length(start.line) == 0) return(NA)
  start.line <- start.line[1]
  if(sbj.line < fil.line)
  {
    sbj_info <- webpage[sbj.line:fil.line]
    fil_info <- webpage[fil.line:start.line]
  }
  if(fil.line < sbj.line)
  {
    sbj_info <- webpage[sbj.line:start.line]
    fil_info <- webpage[fil.line:sbj.line]
  }
  
  sbj_info <- paste0(sbj_info, collapse = "\n")
  fil_info <- paste0(fil_info, collapse = "\n")
  return(fil_info)
}
### here I put files in to SQL
put.into.db <- function(name_out, master, con)
{
  con_out <- dbConnect(SQLite(), name_out)
  dbSendQuery(conn = con_out, 
              "CREATE TABLE filings
              (FILENAME TEXT, FILING TEXT, SBJ TEXT, FIL TEXT, LINK TEXT,
              DATE TEXT, TYPE TEXT)")
  
  files <- dbGetQuery(con, 'SELECT FILENAME FROM compsubm')
  N <- length(files$FILENAME)
  step <- 10000
  
  for(i in 1:(N %/% step + 1))
  {
    print(i)
    print(Sys.time())
    ### read step amount of observations
    end <- (i-1)*step
    start <- min(step,N - end)
    line <- paste0('SELECT * FROM compsubm LIMIT ',start, ' OFFSET ', end)
    x = dbGetQuery(con, line)
    
    ### create a table with clean SC13, subject and files info
    body_text <- lapply(x$COMLSUBFILE, get_body)
    sbj_text <- lapply(x$COMLSUBFILE, get_sbj)
    fil_text <- lapply(x$COMLSUBFILE, get_fil)
    
    df <- NULL
    df$FILENAME <- as.character(x$FILENAME)
    df <- as.data.frame(df)
    body_text <- as.vector(body_text)
    df$FILING <- unlist(body_text)
    df$FILING  <- as.character(df$FILING)
    df$SBJ <- unlist(sbj_text)
    df$FIL <- unlist(fil_text)
    ### add link to a filing webpage
    df$LINK <- master$address[match(df$FILENAME, master$file)]
    df$DATE <- master$date[match(df$FILENAME, master$file)]
    df$TYPE <- master$type[match(df$FILENAME, master$file)]   
    dbWriteTable(con_out, name = "filings", df, append = T)
  }
  
  dbDisconnect(con_out)
  return(1)
}
dates <- get_dates(start_year, start_QTR, end_year, end_QTR)
dates$year_QTR <- paste0(dates$year, dates$QTR)

for(yearqtr in dates$year_QTR)
{
  print(Sys.time())
  print(yearqtr)
  master <- fread(paste0("./Master/master_", yearqtr, ".csv"))
  master[, address := paste0(gsub("(-)|(.txt)","",link),"/",file)]
  dbname <- paste0("./Forms/",yearqtr, ".sqlite")
  con = dbConnect(SQLite(), dbname = dbname)
  name_out <- paste0("./Clean Forms/sc13_", yearqtr, ".sqlite")
  put.into.db(name_out, master, con)
  dbDisconnect(con)
}

