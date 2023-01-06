###############################################################################
#################### Setting up ###############################################
###############################################################################
### folder with Parsed Forms
in_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Parsed Forms/"
out_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Shared_Files/"
### path to CRSP-Compustat merged file
crsp_comp_path <- "/Users/evolkova/Dropbox/DataY/Compustat/crsp_compustat_1990_2021.csv"
crsp_msf_path <- "/Users/evolkova/Dropbox/DataY/CRSP/MSF/CRSP_MSF.csv"
last_year <- 2021
require(pacman)
p_load(data.table, lubridate, dplyr, stringr, Hmisc, httr)

###############################################################################
############### Creating company-block-year table #############################
###############################################################################


### reading all forms, putting them in a table and converting dates
forms <- list.files(in_dir, full.names = T) %>% 
  str_subset("rds") %>% 
  lapply(readRDS) %>%
  rbindlist %>% 
  mutate(DATE = ymd(DATE)) %>% 
  mutate(FILING_YEAR = year(DATE), YEAR = year(DATE))

### this is a date when all the forms for the previous year should be filed
forms[, cut_of_date := ymd(paste0(FILING_YEAR, "-02-14"))]

### if investor files 13G and holds below 10% he files before Feb 14th I give them 5 "extra" days of delay
forms[grepl("SC 13G", TYPE) & max_prc < 10 & DATE - cut_of_date <= 10, YEAR := FILING_YEAR - 1]

forms[, cut_of_date := ymd(paste0(FILING_YEAR, "-01-01"))]
forms[DATE - cut_of_date <= 14, YEAR := FILING_YEAR - 1]
forms[, `:=` (cut_of_date = NULL, prc = NULL, FILING_YEAR = NULL)]

forms <- forms[max_prc > 4.5 & !is.na(forms$fil_CIK)]
forms[, dif := ymd(paste0(YEAR, "-01-01")) - DATE]
setkey(forms, fil_CIK, sbj_CIK, YEAR, dif)

forms <- forms[!duplicated(paste0(fil_CIK, sbj_CIK, YEAR))]

### some of the companies do not file forms every year
### for these companies I use the value of the previous years
add_gaps <- function(forms, step) {
  forms[, gap:=  shift(YEAR, 1, type = "lead") - YEAR, by = c("fil_CIK", "sbj_CIK")]
  add <- forms[gap == step]
  add[, YEAR := YEAR + 1]
  forms <- rbind(forms, add)
  setkey(forms, fil_CIK, sbj_CIK, YEAR)
  return(forms)
}

forms <- forms %>% add_gaps(4) %>% add_gaps(3) %>% add_gaps(2) %>% mutate(dif = NULL, gap = NULL)

### forming annual file
annual <- forms[YEAR <= last_year]
setkey(annual, sbj_CIK, YEAR, fil_CIK)

###############################################################################
#################### Adding 13F filing information ############################
###############################################################################

start_date <- forms$DATE %>% min %>% floor_date("quater")
end_date <- paste0(last_year, "1231") %>% ymd 
dates <- seq(min(forms$DATE), max(forms$DATE), by = "quarters")


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
    filter(grepl("13F", type)) %>%
    mutate(link = paste0("https://www.sec.gov/Archives/", link)) %>%
    mutate(file = gsub(".*/", "", link))
  
  
  closeAllConnections()
  file.remove(paste0(out_dir, "tmp.txt"))
  return(master)
}

master <- lapply(dates, qtr.master.file) %>% rbindlist %>% mutate(year = year(ymd(date)))

annual$files_13F <- 0
annual$files_13F[paste(as.numeric(annual$fil_CIK), annual$YEAR) %in% paste(master$cik, master$year)] <- 1

###############################################################################
#################### Match CRSP-COMPUSTAT INFO ############################
###############################################################################

### matching permno and cusip to annual file
comp <- fread(crsp_comp_path, select = c("cik", "LPERMNO", "cusip", "fyear", "fyr"))
crsp_monthly <- fread(crsp_msf_path, select = c("PERMNO", "COMNAM", "RET", "PRC", "date", "CUSIP"))
match <- match(as.numeric(annual$sbj_CIK), comp$cik)
annual$Permno <- comp$LPERMNO[match]
annual$cusip_comp <- comp$cusip[match]


crsp_monthly <- crsp_monthly[!is.na(RET) & !is.na(PERMNO) & (str_length(COMNAM) > 2)]
crsp_monthly[, cusip6 := substr(CUSIP, 1, 6)]
match <- match(annual$CUSIP6, crsp_monthly$cusip6)
annual$Permno <- crsp_monthly$PERMNO[match]
annual$marcap <- crsp_monthly$SHROUT[match]*abs(crsp_monthly$PRC[match])

match <- match(substr(annual$cusip_comp[is.na(annual$Permno)],1,6), crsp_monthly$cusip6)
annual$Permno[is.na(annual$Permno)] <- crsp_monthly$PERMNO[match]

match <- match(annual$Permno, crsp_monthly$PERMNO)
annual$sbj_cname_crsp <- crsp_monthly$COMNAM[match]


### calculate statistics for aggregate ownership, HHI, # blocks, etc
annual[, `:=` (num_block = length(unique(fil_CIK)), block_hold = sum(max_prc)), by = c("sbj_CIK", "YEAR")]
annual[, block_portion := max_prc/block_hold]
### identity categories
annual[, `:=` (individual = 0, active_inst = 0, passive_inst = 0, other = 0)]
annual[item12 == "in", individual := 1]
annual[grepl("13D", TYPE) & files_13F == 1 & individual == 0, active_inst := 1]
annual[grepl("13G", TYPE) & files_13F == 1 & individual == 0, passive_inst := 1]
annual[, other := 1 - individual - active_inst - passive_inst]

annual <- annual %>% filter(sbj_CIK != "0000000000")
fwrite(annual, paste0(out_dir, "annual.csv"))

out <- annual %>% select("fil_CIK", "fil_CNAME", "sbj_CNAME", "sbj_CIK", "YEAR" ,"max_prc", "individual", "active_inst", "passive_inst", "other", "files_13F") 
out[, ever_filed_13F := max(files_13F), by = fil_CIK]

non_inst <- out[ever_filed_13F == 0 & active_inst == 0 & passive_inst == 0] %>% select("fil_CIK", "fil_CNAME", "sbj_CIK",  "sbj_CNAME","YEAR" ,"max_prc")#, "individual", "other")
colnames(non_inst) <- c("blockholder_CIK", "blockholder_name", "company_CIK", "company_name", "year", "position")#, "individual", "other")
setkey(non_inst, blockholder_CIK, company_CIK, year)
fwrite(non_inst, paste0(out_dir, "non_inst_blocks.csv"))



all_blocks <- out %>% select("fil_CIK", "fil_CNAME", "sbj_CIK",  "sbj_CNAME","YEAR" ,"max_prc")#, "individual", "other")
colnames(all_blocks) <- c("blockholder_CIK", "blockholder_name", "company_CIK", "company_name", "year", "position")#, "individual", "other")
setkey(all_blocks, blockholder_CIK, company_CIK, year)
fwrite(all_blocks, paste0(out_dir, "all_blocks.csv"))
