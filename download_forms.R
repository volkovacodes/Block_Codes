require(data.table)
require(Hmisc)
setwd(wd)
###########################################
####### prepare SEC master file ###########
###########################################
load( , file = "./CRSP_COMP/sec_master_1994_2015.rda")
setkey(sec_master,cik)
sec <- sec_master[grep("SC 13(D|G)", form_type)]
sec[, date := as.Date(date)]
sec[, file := gsub(".*/", "", link)]
sec[, file := gsub("\\s","", file)]
sec[, year := year(date)]



sec[, folder := gsub(".txt", "", file)]
sec[, folder := gsub("-","", folder)]
sec[, address := paste0("https://www.sec.gov/Archives/edgar/data/",
                                cik, "/", folder, "/",file)]
write.csv(sec, "sec.csv", row.names = F)
rm(sec_master)


for(year in sort(unique(sec$year)))
{
  dir.create(file.path("./Master Files/", year), showWarnings = FALSE)
}

###########################################
####### download all SC13 forms ###########
#######     ~20 hours    ##################
###########################################

for(look_year in 1994:2015)
{
  print(Sys.time())
  print(look_year)
  dir <- paste0("./Master Files/", look_year)
  setwd(dir)
  year_table <- sec[year == look_year]
  year_table[, address := gsub("\\s","", address)]
  N <- length(year_table$date)
  print(N)
  for(i in 1:N)
  {
    try(download.file(year_table$address[i], year_table$file[i], quiet = T))
  }
}

###########################################
####### put all forms in SQdatabase #######
###########################################

library(DBI)
library(RSQLite)
together <- function(x)
{
  return(paste(x, collapse = "\n"))
}
setwd("./Comptele_Submission_Files/")

for(year in 1994:2015)
{
  start_time <- Sys.time()
  print(year)
  dbname <- paste0(year, ".sqlite")
  con = dbConnect(SQLite(), dbname=dbname)
  dbSendQuery(conn=con,
              "CREATE TABLE compsubm
            (FILENAME TEXT, COMLSUBFILE TEXT)")
  path <- paste0("./Master Files/", year, "/")
  files <- list.files(path)
  n <- length(files)
  step <- 100
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
  end_time <- Sys.time()
  print(end_time - start_time)
}


