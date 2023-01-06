###############################################################################
#################### Setting up ###############################################
###############################################################################
### folder with Parsed Forms
out_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Parsed Forms/"
### folder with Parsed Forms
ins_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Insider_Filings/"
### path to CRSP-Compustat merged file
crsp_comp_path <- "/Users/evolkova/Dropbox/DataY/Compustat/crsp_compustat_1990_2021.csv"
crsp_msf_path <- "/Users/evolkova/Dropbox/DataY/CRSP/MSF/CRSP_MSF.csv"
last_year <- 2021
redo <- F ### do we need to re-do insider master file?

require(pacman)
p_load(data.table, lubridate, dplyr, stringr, Hmisc, httr, rvest, magrittr, parallel)


### annual file
annual <- fread(paste0(out_dir, "annual.csv"))

### crsp file
comp <- fread(crsp_comp_path, select = c("LPERMNO", "cik", "fyear")) 

### compustat file
crsp <- fread(crsp_msf_path, select = c("PERMNO", "SHROUT", "date", "RET"))
crsp$cik <- comp$cik[match(crsp$PERMNO, comp$LPERMNO)]
crsp <- crsp %>% 
  filter(!is.na(cik)) %>% 
  filter(!is.na(SHROUT)) %>% 
  filter(date >= 19940101) %>% 
  mutate(date = ymd(date)) %>% 
  mutate(year_month = floor_date(date, unit = "month"))

###############################################################################
###################### Download master insider  ###############################
###############################################################################

start_date <- annual$DATE %>% ymd %>% min %>% floor_date("quater")
end_date <- annual$DATE %>% max
dates <- seq(start_date, end_date, by = "quarters")


qtr.master.file <- function(date) {
  
  master.link <- paste0("https://www.sec.gov/Archives/edgar/full-index/", year(date), "/QTR", quarter(date), "/master.idx")
  print(sprintf("Downloading master file for quarter %d of year %s...", quarter(date), year(date)))
  
  download.file(master.link, paste0(out_dir, "tmp.txt"),
                headers = c("User-Agent" = "Ekaterina Volkova orhahog@gmail.com")
  )
  
  master <- paste0(out_dir, "tmp.txt") %>%
    readLines() %>%
    gsub("#", "", .) %>%
    paste0(collapse = "\n") %>%
    fread(sep = "|", skip = 11) %>%
    `colnames<-`(c("cik", "name", "type", "date", "link")) %>%
    filter(grepl(form_type, type)) %>%
    mutate(link = paste0("https://www.sec.gov/Archives/", link)) %>%
    mutate(file = gsub(".*/", "", link))
  
  
  closeAllConnections()
  file.remove(paste0(out_dir, "tmp.txt"))
  return(master)
}

form_type <- "^3$|^4$|^5$"
if(redo == T) {
  master <- lapply(dates, qtr.master.file) %>% rbindlist %>% mutate(year = year(ymd(date)))
  saveRDS(master, paste0(out_dir, "master_insider.rds"))
}

if(redo == F) master <- readRDS(paste0(out_dir, "master_insider.rds"))
###############################################################################
###################### Download master insider  ###############################
###############################################################################


get_insider_trades <- function(cik){
  
  filename <- paste0(ins_dir, cik, ".csv")
  if(file.exists(filename)) {
    #return(0)
    initial_ownership <- fread(filename)
    
    if(dim(initial_ownership)[1] < 2) initial_ownership <- NULL
    start_date <- max(initial_ownership[[2]])
    if(is.na(start_date) | is.infinite(start_date)) start_date <- ymd("19940101")
    initial_ownership[[2]] <- as.character(initial_ownership[[2]])
  }
  print(cik)
  if(!file.exists(filename)) {
    initial_ownership <- NULL
    start_date <- ymd("19940101")
  }
  
  page <- cik %>% 
    sprintf("%08d",.) %>% 
    paste0("https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK=",.) %>% 
    read_html 
  
  ownership <- page %>% 
    html_table %>% 
    extract2(8) %>% 
    data.table %>% try
  
  if(max(grepl("try-error",class(ownership))) == T) {
    ownership <- data.table("NULL")
    #fwrite(initial_ownership, filename)
    return(0)
  }
  ownership <- ownership[which(ownership[[2]] %>% ymd > start_date)]
  min_date <- ownership[[2]] %>% ymd %>% min(.,na.rm = T)
  if(is.na(min_date) | is.infinite(min_date)) return(initial_ownership)
  next80 <- page %>% html_node("form") %>% as.character
  
  #& min_date >= start_date
  while(max(grepl("Next 80", next80)) == T & min_date > start_date){
    next80 <- next80[grepl("Next 80", next80)]
    next80 <- gsub(".*/cgi-bin/", "https://www.sec.gov/cgi-bin/", next80)
    next80 <- gsub("'\\\\.*", '', next80)
    next80 <- gsub("'.*", "", next80)
    
    page <- next80 %>% read_html
    
    tmp <- try(page %>% 
                 html_table %>% 
                 extract2(8) %>% 
                 data.table)
    
    if(max(class(tmp) == "try-error") == T) tmp <- NULL
    tmp <- tmp[ymd(tmp[[2]]) > start_date]
    
    
    min_date <- tmp[[2]] %>% ymd %>% min(.,na.rm = T)
    if(is.na(min_date) | is.infinite(min_date)) break
    ownership <- rbind(ownership, tmp)
    next80 <- page %>% html_node("form") %>% as.character
    
  }
  
  for(i in 1:dim(ownership)[2]) ownership[[i]] <- ownership[[i]] %>% as.character
  for(i in 1:dim(initial_ownership)[2]) initial_ownership[[i]] <- initial_ownership[[i]] %>% as.character
  
  ownership <- rbind(ownership, initial_ownership)
  return(ownership)
  fwrite(ownership, filename)
}

### collect all 
ciks <- master[cik %in% crsp$cik]$cik %>% unique %>% sort
x <- lapply(ciks, get_insider_trades)
