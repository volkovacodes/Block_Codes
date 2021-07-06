start_QTR <- "2018Q4"
end_QTR <- "2018Q4"

### use this dir to download files
out_dir <- "/Users/evolkova/Documents/Blocks/"
if(!dir.exists(paste0(out_dir, "Clean Forms/"))) dir.create(paste0(out_dir, "Clean Forms/"))


get_dates <- function(start_QTR, end_QTR) {
  require(data.table)
  require(zoo)
  require(lubridate)
  require(dplyr)

  end_QTR <- end_QTR %>%
    as.yearqtr("%YQ%q") %>%
    as.Date()

  all_dates <- start_QTR %>%
    as.yearqtr("%YQ%q") %>%
    as.Date()

  while (all_dates[length(all_dates)] < ymd(end_QTR)) {
    all_dates <- c(all_dates, all_dates[length(all_dates)] %m+% months(3))
  }

  return(all_dates)
}

### this function is taken from iangow github
html2txt <- function(file) {
  require(XML)
  xpathApply(htmlParse(file, encoding = "UTF-8"), "//body", xmlValue)[[1]]
}
### this function extracts text from txt/htm
### of the first filings file in the sequence
get_body <- function(webpage) {
  require(Hmisc)
  require(stringr)

  webpage <- unlist(strsplit(webpage, "\n"))
  file.name <- gsub("<FILENAME>", "", grep("<FILENAME>.*$", webpage, perl = TRUE, value = TRUE))
  start.line <- grep("<DOCUMENT>.*$", webpage, perl = TRUE)
  end.line <- grep("</DOCUMENT>.*$", webpage, perl = TRUE)

  if (length(start.line) * length(end.line) == 0) {
    return(NA)
  }

  if (length(file.name) == 0) {
    return(paste0(webpage[start.line:end.line], collapse = "\n"))
  }

  file.ext <- tolower(gsub(".*\\.(.*?)$", "\\1", file.name[1]))

  start.line <- start.line[1]
  end.line <- end.line[1]

  if (file.ext %in% c("htm", "xls", "xlsx", "js", "css", "paper", "xsd")) {
    temp <- webpage[start.line:end.line]
    pdf.start <- grep("^<TEXT>", temp, perl = TRUE) + 1
    pdf.end <- grep("^</TEXT>", temp, perl = TRUE) - 1
    res <- try(text <- html2txt(temp[pdf.start:pdf.end]), silent = T)
    if (class(res) == "try-error") text <- temp[pdf.start:pdf.end]
  }

  if (file.ext == "txt") {
    text <- webpage[start.line:end.line]
    text <- paste0(text, collapse = "\n")
  }
  return(text)
}

### here I extract sbj info
get_sbj <- function(webpage) {
  webpage <- unlist(strsplit(webpage, "\n"))
  sbj.line <- grep("SUBJECT COMPANY:", webpage)
  fil.line <- grep("FILED BY:", webpage)
  start.line <- grep("<DOCUMENT>.*$", webpage, perl = TRUE)
  if (length(sbj.line) * length(fil.line) * length(start.line) == 0) {
    return(NA)
  }
  start.line <- start.line[1]
  if (sbj.line < fil.line) {
    sbj_info <- webpage[sbj.line:fil.line]
  }

  if (fil.line < sbj.line) {
    sbj_info <- webpage[sbj.line:start.line]
  }


  sbj_info <- paste0(sbj_info, collapse = "\n")
  return(sbj_info)
}
### here I extract fil info
get_fil <- function(webpage) {
  webpage <- unlist(strsplit(webpage, "\n"))
  sbj.line <- grep("SUBJECT COMPANY:", webpage)
  fil.line <- grep("FILED BY:", webpage)
  start.line <- grep("<DOCUMENT>.*$", webpage, perl = TRUE)
  if (length(sbj.line) * length(fil.line) * length(start.line) == 0) {
    return(NA)
  }

  start.line <- start.line[1]
  if (sbj.line < fil.line) {
    sbj_info <- webpage[sbj.line:fil.line]
    fil_info <- webpage[fil.line:start.line]
  }

  if (fil.line < sbj.line) {
    sbj_info <- webpage[sbj.line:start.line]
    fil_info <- webpage[fil.line:sbj.line]
  }

  sbj_info <- paste0(sbj_info, collapse = "\n")
  fil_info <- paste0(fil_info, collapse = "\n")
  return(fil_info)
}
### here I put files in to SQL
put.into.db <- function(name_out, master, con) {
  require(DBI)
  require(RSQLite)
  con_out <- dbConnect(SQLite(), name_out)
  dbSendQuery(
    conn = con_out,
    "CREATE TABLE filings
              (FILENAME TEXT, FILING TEXT, SBJ TEXT, FIL TEXT, LINK TEXT,
              DATE TEXT, TYPE TEXT)"
  )

  files <- dbGetQuery(con, "SELECT FILENAME FROM compsubm")
  N <- length(files$FILENAME)
  step <- 10000

  for (i in 1:(N %/% step + 1))
  {
    print(i)
    print(Sys.time())
    ### read step amount of observations
    end <- (i - 1) * step
    start <- min(step, N - end)
    line <- paste0("SELECT * FROM compsubm LIMIT ", start, " OFFSET ", end)
    x <- dbGetQuery(con, line)

    ### create a table with clean SC13, subject and files info
    body_text <- lapply(x$COMLSUBFILE, get_body)
    sbj_text <- lapply(x$COMLSUBFILE, get_sbj)
    fil_text <- lapply(x$COMLSUBFILE, get_fil)

    df <- NULL
    df$FILENAME <- as.character(x$FILENAME)
    df <- as.data.frame(df)
    body_text <- sapply(body_text, function(x) paste0(x, collapse = " "))
    df$FILING <- body_text
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

dates <- get_dates(start_QTR, end_QTR)

for (i in 1:length(dates)) {
  print(Sys.time())
  print(paste0("year: ", year(dates[i]), ", quarter: ", quarter(dates[i])))
  master <- fread(paste0(out_dir, "Master/master_", year(dates[i]), quarter(dates[i]), ".csv"))
  master[, address := paste0(gsub("(-)|(.txt)", "", link), "/", file)]
  dbname <- paste0(out_dir, "Forms/", year(dates[i]), quarter(dates[i]), ".sqlite")
  con <- dbConnect(SQLite(), dbname = dbname)
  name_out <- paste0(out_dir, "Clean Forms/sc13_", year(dates[i]), quarter(dates[i]), ".sqlite")
  put.into.db(name_out, master, con)
  dbDisconnect(con)
}
