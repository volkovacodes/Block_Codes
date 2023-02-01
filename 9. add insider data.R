###############################################################################
#################### Setting up ###############################################
###############################################################################
### folder with Parsed Forms
out_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Shared_Files/"
### folder with Parsed Forms
ins_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Insider_Filings/"
### path to CRSP-Compustat merged file
crsp_comp_path <- "/Users/evolkova/Dropbox/DataY/Compustat/crsp_compustat_1990_2021.csv"
crsp_msf_path <- "/Users/evolkova/Dropbox/DataY/CRSP/MSF/CRSP_MSF.csv"
last_year <- 2021
redo <- F ### do we need to re-do insider master file?

require(pacman)
p_load(data.table, lubridate, dplyr, stringr, Hmisc, httr, rvest, magrittr, parallel)

master_13F <- fread(paste0(out_dir, "master_13F.csv"))


annual <- fread(paste0(out_dir, "all_blocks.csv"))
annual[, id := paste(company_CIK, blockholder_CIK, year)]
annual[, files_13F := 0]
annual[blockholder_CIK %in% master_13F$cik, files_13F := 1]

###############################################################################
#################### Getting number of SHROUT ##################################
###############################################################################
comp <- fread(crsp_comp_path, select = c("LPERMNO", "cik", "fyear")) 
crsp <- fread(crsp_msf_path, select = c("PERMNO", "SHROUT", "date", "RET"))
crsp$cik <- comp$cik[match(crsp$PERMNO, comp$LPERMNO)]
crsp <- crsp %>% 
  filter(!is.na(cik)) %>% 
  filter(!is.na(SHROUT)) %>% 
  filter(date >= 19940101) %>% 
  mutate(date = ymd(date)) %>% 
  mutate(year_month = floor_date(date, unit = "month"))


find_blocks <- function(fl){
  ck <- fl %>% gsub(".*/","",.) %>% gsub(".csv","",.) %>% as.numeric()

  ownership <- fl %>% 
    fread 
  
  if(dim(ownership)[1] < 2) return(NULL)
  tmp <- crsp[cik == ck] %>% select("SHROUT", "year_month")
  
  ownership <- ownership %>% 
    setnames(c("A_D", "TrDate", "ExDate", "blockholder_name", "Form", "Type", "D_I", "Num_Tr", "Num_Own", "Line", "blockholder_CIK", "Security")) %>% 
    filter(Security == "Common Stock") %>% 
    mutate(date = ymd(TrDate)) %>% 
    mutate(year_month = floor_date(date, unit = "month")) %>% 
    inner_join(tmp, by = "year_month") %>% 
    mutate(prc_own = 100*(as.numeric(Num_Own))/(10^3*SHROUT))
  
  
  ownership[, max_prc := max(prc_own),by = "blockholder_CIK"]
  
  ownership <- ownership[max_prc >= 4.5] %>% select(date, prc_own, blockholder_CIK, blockholder_name) 
  ownership[, year := year(date)]
  setkey(ownership, blockholder_CIK, date)
  
  ownership <- ownership[,list(position = max(prc_own), blockholder_name = blockholder_name[1]), by = "blockholder_CIK,year"]
  ownership$company_CIK <- ck
  return(ownership)
}

files <- list.files(ins_dir, full.names = T)
blocks <- files %>% lapply(find_blocks) %>% rbindlist()


###############################################################################
###################### Loading master file  ###################################
###############################################################################

all_master <- readRDS(paste0(out_dir, "master_insider.rds"))
all_master[, n := .N, by = file]
master <- all_master[n > 2]
master[, company := 0]
master[cik %in% blocks$company_CIK, company := 1]
master[, max_company := max(company), by = file]
master <- master[max_company == 1]

master <- master[, list(all_blockholder_CIK = paste0(cik[company == 0],collapse = "|"), 
                   company_CIK = cik[which(company == 1)]), by = file]

master <- master[, list(blockholder_CIK = all_blockholder_CIK %>% str_split(pattern = "\\|") %>% unlist %>% as.numeric), by = "all_blockholder_CIK,company_CIK"]



blocks$all_blockholder_CIK <- master$all_blockholder_CIK[match(paste(blocks$blockholder_CIK, blocks$company_CIK), paste(master$blockholder_CIK, master$company_CIK))]
blocks[is.na(all_blockholder_CIK), all_blockholder_CIK := blockholder_CIK]  
blocks <- blocks[,list(blockholder_CIK = all_blockholder_CIK %>% str_split(pattern = "\\|") %>% unlist %>% as.numeric), by = "blockholder_name,company_CIK,position,year,all_blockholder_CIK"]
blocks[, id := paste(company_CIK,blockholder_CIK,year)]

blocks[, in_prev := 0]
blocks[id %in% annual$id, in_prev := 1]
blocks[, have := max(in_prev), by = "company_CIK,all_blockholder_CIK,year"]

blocks[, ind := paste(company_CIK, all_blockholder_CIK, year)]
blocks <- blocks[!duplicated(ind)]
blocks[(have == 0) & (year %in% 1996:2021) & (company_CIK %in% crsp$cik)]$ind %>% unique %>% length 

blocks$company_name <- all_master$name[match(blocks$company_CIK, all_master$cik)]
addon <- blocks[(have == 0) & (year %in% 1996:2021) & (company_CIK %in% crsp$cik) & position > 5]#$ind %>% unique %>% length 
addon <- addon[!duplicated(ind) & year %in% 1994:2021] %>% select("blockholder_CIK", "blockholder_name", "company_CIK", "company_name", "year", "position")

fwrite(addon, paste0(out_dir, "insider_addon.csv"))

addon$files_13F <- 0
annual$id <- NULL
out <- rbind(annual, addon)
setkey(out, company_CIK, blockholder_CIK, year)
out[, position := format(round(position, 2), nsmall = 2)]
fwrite(out, paste0(out_dir, "blockholders.csv"))

