require(DBI)
require(RSQLite)
require(XML)
require(data.table)
require(stringr)
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
  if(length(file.name) == 0) return(NA)
  file.ext <- tolower(gsub(".*\\.(.*?)$", "\\1", file.name[1]))
  start.line <- grep("<DOCUMENT>.*$", webpage,  perl=TRUE)
  end.line <- grep("</DOCUMENT>.*$", webpage,  perl=TRUE) 
  if(length(start.line)*length(end.line) == 0) return(NA)
  start.line <- start.line[1]
  end.line <- end.line[1]
  if (file.ext %in% c("htm", "xls", "xlsx", "js", "css", "paper", "xsd")) 
  {
    temp <- webpage[start.line:end.line]
    pdf.start <- grep("^<TEXT>", temp,  perl=TRUE) +1
    pdf.end <- grep("^</TEXT>", temp,  perl=TRUE) -1  
    res <- try(text <- html2txt(temp[pdf.start:pdf.end]))
    if(class(res) == "try-error") text <- NA
    
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


for(cyear in 1996:2016)
{
  sec <- fread("sec.csv")
  year_sec <- sec[year == cyear]  
  rm(sec)
  dbname <- paste0(cyear, ".sqlite")
  con = dbConnect(SQLite(), dbname = dbname)
  name_out <- paste0("sc13_", cyear, ".sqlite")
  con_out <- dbConnect(SQLite(), name_out)
  dbSendQuery(conn = con_out, 
              "CREATE TABLE filings
              (FILENAME TEXT, FILING TEXT, SBJ TEXT, FIL TEXT, LINK TEXT)")
  
  files <- dbGetQuery(con, 'SELECT FILENAME FROM compsubm')
  N <- length(files$FILENAME)
  step <- 1000
  
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
    df$LINK <- year_sec$address[match(df$FILENAME, year_sec$file)]
    
    dbWriteTable(con_out, name = "filings", df, append = T)
  }
  
  dbDisconnect(con_out)
  dbDisconnect(con)
}

