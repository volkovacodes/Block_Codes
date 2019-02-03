start_year <- 2017
start_QTR <- 1

end_year <- 2018
end_QTR <- 8
setwd("/Volumes/ORHAHOG_USB/Blocks/")
require(data.table)
require(Hmisc)
require(data.table)

###########################################
####### construct SEC master file ########
###########################################
qtr.master.file <- function(year, QTR)
{
  require(data.table)
  name <- paste0("https://www.sec.gov/Archives/edgar/full-index/", year,"/QTR",QTR,"/master.idx")
  print(sprintf("Downloading master file for quarter %d of year %s...", QTR, year))
  master <- readLines(url(name))
  master <- gsub("#", "", master) # R does not treat a comment sign well
  start_ind <- grep("CIK|Company Name", master)[1]
  master <- master[(start_ind+2):length(master)]
  write(master, "tmp.csv")
  master_table <- fread("tmp.csv", sep = "|")
  rm(master)
  colnames(master_table) <- c("cik", "name", "type", "date", "link")
  master_table <- as.data.table(master_table)
  master_table <- master_table[grep("SC 13(D|G)", type)]
  master_table[, link := paste0("https://www.sec.gov/Archives/", link)]
  master_table[, file := gsub(".*/", "", link)]
  file.remove("tmp.csv")
  closeAllConnections()
  return(master_table)
}
###########################################
#### download all files into temp dir  ####
###########################################
dwnld.files <- function(master)
{
  require(RCurl)
  dir.create("temp_dir")
  master <- as.data.frame(master)
  master <- master[!duplicated(master$file),]
  for(j in 1:length(master$file))
  {
    file <- NA
    file_url <- as.character(master$link[j])
    file_name <- paste0("./temp_dir/",master$file[j])
    try(file <- getURL(file_url))
    write(file, file_name)
  }
}
###########################################
####### put all forms in SQdatabase #######
###########################################
put.files.in.sql <- function(dbname)
{
  library(DBI)
  library(RSQLite)
  together <- function(x)
  {
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
  for(i in 1:(n %/% step + 1))
  {
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
