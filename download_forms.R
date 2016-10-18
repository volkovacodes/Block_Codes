require(data.table)
require(Hmisc)
#setwd(wd)
###########################################
####### construct SEC master file ########
###########################################
annual.master.file <- function(year)
{
  require(data.table)
  master_total <- NULL
  for(i in 1:4)
  {
    name <- paste0("ftp://ftp.sec.gov/edgar/full-index/",year,"/QTR",i,"/master.idx")
    print(sprintf("Downloading master file for quarter %d of year %s...", i, year))
    master <- readLines(url(name))
    master <- master[grep("SC 13(D|G)", master)]
    master_table <- read.table(textConnection(master), sep = "|")
    rm(master)
    colnames(master_table) <- c("cik", "name", "type", "date", "link")
    master_table <- as.data.table(master_table)
    master_table[, link := paste0("https://www.sec.gov/Archives/", link)]
    master_table[, file := gsub(".*/", "", link)]
    master_total <- rbind(master_total, master_table)
    closeAllConnections()
  }
  return(master_total)
}
###########################################
#### download all files into temp dir  ####
###########################################
dwnld.files <- function(master)
{
  dir.create("temp_dir")
  for(i in 1:length(master$file))
  {
    try(download.file(master$link[i], paste0("./temp_dir/",master$file[i]), quiet = T))
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

for(year in 2001:2003)
{
  master <- annual.master.file(year)
  write.csv(master, paste0("master_", year, ".csv"))
  dwnld.files(master)
  put.files.in.sql(paste0(year, ".sqlite"))
}



