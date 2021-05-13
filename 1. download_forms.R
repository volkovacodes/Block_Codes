start_year <- 2017
start_QTR <- 1

end_year <- 2018
end_QTR <- 8
setwd("/Volumes/ORHAHOG_USB/Blocks/")

###########################################
####### construct SEC master file ########
###########################################
qtr.master.file <- function(year, QTR){
  
  require(data.table)
  require(dplyr)
  require(RCurl)
  require(Hmisc)
  
  master.link <- paste0("https://www.sec.gov/Archives/edgar/full-index/", year,"/QTR",QTR,"/master.idx")
  print(sprintf("Downloading master file for quarter %d of year %s...", QTR, year))
  
  
  master <- master.link %>%
    getURL() %>%
    gsub("#", "",.) %>%
    fread(sep = "|", skip = 11) %>% 
    `colnames<-`(c("cik", "name", "type", "date", "link")) %>%
    filter(grepl("SC 13(D|G)", type)) %>%
    mutate(link = paste0("https://www.sec.gov/Archives/", link)) %>%
    mutate(file = gsub(".*/","",link))


  closeAllConnections()
  return(master)
}
###########################################
#### download all files into temp dir  ####
###########################################
### sometimes the SEC puts a limit on the number of downloads
### put delay = T to account for that
dwnld.files <- function(master, delay = T){
  require(RCurl)
  dir.create("temp_dir")
  master <- master[!duplicated(file)]
  
  for(j in 1:length(master$file)){
    if(delay = T) Sys.sleep(0.11)
    file_name <- paste0("./temp_dir/",master$file[j])
    
    file <- master$link[j] %>%
      getURL %>%
      try

    write(file, file_name)
  }
}
###########################################
####### put all forms in SQdatabase #######
###########################################
put.files.in.sql <- function(dbname){
  library(DBI)
  library(RSQLite)
  together <- function(x){
    return(paste(x, collapse = "\n"))
  }
  
  con = dbConnect(SQLite(), dbname=dbname)
  dbSendQuery(conn=con,
              "CREATE TABLE compsubm
              (FILENAME TEXT, COMLSUBFILE TEXT)")
  path <- paste0("./temp_dir/")
  files <- list.files(path)
  n <- length(files)
  step <- 500
  for(i in 1:(n %/% step + 1)){
    start <- 1 + (i-1)*step
    end <- i*(step)
    ind <- start:min(end,n)
    objects <- lapply(paste0(path,files[ind]), readLines)
    clean <- lapply(objects, together)
    data <- NULL
    data$FILENAME <- files[ind]
    data <- as.data.frame(data)
    data$COMLSUBFILE <- unlist(clean)
    dbWriteTable(conn=con, name = "compsubm", data, append = T)
  }
  dbDisconnect(con)
  unlink("temp_dir", recursive = T)
}

get_dates <- function(start_year, start_QTR, end_year, end_QTR){
  require(data.table)
  all_dates <- data.table(year = rep(1993:2050, 4))
  setkey(all_dates,year)
  all_dates[, QTR := 1:.N, by = year]
  all_dates <- as.data.frame(all_dates)
  
  x <- paste0(all_dates$year, all_dates$QTR) >= paste0(start_year, start_QTR) & paste0(all_dates$year, all_dates$QTR) <= paste0(end_year, end_QTR)
  return(all_dates[x,])
}




dates <- get_dates(start_year, start_QTR, end_year, end_QTR)
for(i in 1:length(dates$QTR))
{
  print(Sys.time())
  master <- qtr.master.file(dates$year[i], dates$QTR[i])
  write.csv(master, paste0("./Master/master_", dates$year[i], dates$QTR[i], ".csv"), row.names = F)
  print("Dowloading files, it takes up to 4 hours")
  dwnld.files(master)
  print("Putting all files into SQL & cleaning")
  put.files.in.sql(paste0("./Forms/",dates$year[i],dates$QTR[i], ".sqlite"))
}
