
start_QTR <- "2018Q4"
end_QTR <- "2018Q4"

out_dir <- "/Users/evolkova/Documents/Blocks/"

################################################################################
####################### construct SEC master file ##############################
################################################################################
qtr.master.file <- function(date) {
  require(data.table)
  require(dplyr)
  require(Hmisc)
  require(httr)

  master.link <- paste0("https://www.sec.gov/Archives/edgar/full-index/", year(date), "/QTR", quarter(date), "/master.idx")
  print(sprintf("Downloading master file for quarter %d of year %s...", quarter(date), year(date)))

  download.file(master.link, paste0(out_dir, "tmp.txt"), 
                headers = c("User-Agent" = "Ekaterina Volkova orhahog@gmail.com"))

  master <- paste0(out_dir, "tmp.txt") %>%
    readLines() %>%
    gsub("#", "", .) %>%
    paste0(collapse = "\n") %>%
    fread(sep = "|", skip = 11) %>%
    `colnames<-`(c("cik", "name", "type", "date", "link")) %>%
    filter(grepl("SC 13(D|G)", type)) %>%
    mutate(link = paste0("https://www.sec.gov/Archives/", link)) %>%
    mutate(file = gsub(".*/", "", link))


  closeAllConnections()
  file.remove(paste0(out_dir, "tmp.txt"))
  return(master)
}
################################################################################
####################download files into tmp dir ################################
################################################################################
### sometimes the SEC puts a limit on the number of downloads
### put delay = T to account for that
dwnld.files <- function(master, delay = T) {
  require(RCurl)
  dir.create(paste0(out_dir,"temp_dir"))
  master <- master[!duplicated(file)]

  for (j in 1:length(master$file)) {
    if (delay == T) Sys.sleep(0.13)
    file_name <- paste0(out_dir,"./temp_dir/", master$file[j])

    download.file(master$link[j], file_name, quiet = T,
                  headers = c("User-Agent" = "Ekaterina Volkova orhahog@gmail.com"))

  }
}
###########################################
####### put all forms in SQdatabase #######
###########################################
put.files.in.sql <- function(dbname) {
  library(DBI)
  library(RSQLite)
  together <- function(x) {
    return(paste(x, collapse = "\n"))
  }

  con <- dbConnect(SQLite(), dbname = dbname)
  dbSendQuery(conn = con, "CREATE TABLE compsubm (FILENAME TEXT, COMLSUBFILE TEXT)")
  
  path <- paste0(out_dir,"/temp_dir/")
  files <- list.files(path)
  n <- length(files)
  step <- 500
  for (i in 1:(n %/% step + 1)) {
    start <- 1 + (i - 1) * step
    end <- i * (step)
    ind <- start:min(end, n)
    
    objects <- lapply(paste0(path, files[ind]), readLines)
    clean <- lapply(objects, together)
    data <- NULL
    data$FILENAME <- files[ind]
    data <- as.data.frame(data)
    data$COMLSUBFILE <- unlist(clean)
    dbWriteTable(conn = con, name = "compsubm", data, append = T)
  }
  dbDisconnect(con)
  unlink("temp_dir", recursive = T)
}

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
  
  while (all_dates[length(all_dates)] < ymd(end_QTR)) 
    all_dates <- c(all_dates, all_dates[length(all_dates)]  %m+% months(3))
  
  return(all_dates)
}


dates <- get_dates(start_QTR, end_QTR)
for (i in 1:length(dates)) {
  print(Sys.time())
  master <- qtr.master.file(dates[i])
  fwrite(master, paste0(out_dir,"/Master/master_", year(dates[i]), quarter(dates[i]), ".csv"), row.names = F)
  print("Dowloading files, it takes up to 4 hours")
  dwnld.files(master)
  print("Putting all files into SQL & cleaning")
  put.files.in.sql(paste0(out_dir,"/Forms/", year(dates[i]), quarter(dates[i]), ".sqlite"))
}
