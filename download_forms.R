require(data.table)
require(Hmisc)
setwd("C:/Users/ev99/Dropbox")
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
write.csv(sec, "C:/Users/ev99/Dropbox/Blockholders/sec.csv", row.names = F)
rm(sec_master)
for(year in sort(unique(sec$year)))
{
  dir.create(file.path("C:/Users/ev99/Dropbox/Blockholders/Master Files/", year), showWarnings = FALSE)
}

for(look_year in 1994:2015)
{
  print(Sys.time())
  print(look_year)
  dir <- paste0("C:/Users/ev99/Dropbox/Blockholders/Master Files/", look_year)
  setwd(dir)
  year_table <- sec[year == look_year]
  N <- length(year_table$date)
  print(N)
  for(i in 1:N)
  {
    try(download.file(year_table$address[i], year_table$file[i], quiet = T))
  }
}



