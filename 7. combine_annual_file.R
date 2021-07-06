out_dir <- "/Users/evolkova/Documents/Blocks/Parsed Forms/"

require(data.table)
require(lubridate)
### reading all forms
files <- list.files(out_dir)
files <- files[grepl("rds", files)]
forms <- NULL
for(fl in files) forms <- rbind(forms, readRDS(paste0(out_dir, fl)))

forms[, DATE := ymd(DATE)]
forms[, FILING_YEAR := year(DATE)]
### this is a date when all the forms for the previous year 
### should be filed
forms[, cut_of_date := ymd(paste0(FILING_YEAR, "-02-14"))]
forms[, YEAR := FILING_YEAR]
### if investor files 13G and holds below 10% he files before Feb 14th
### i give them 5 "extra" days of delay
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

forms <- add_gaps(forms, 4)
forms <- add_gaps(forms, 3)
forms <- add_gaps(forms, 2)
forms[, `:=` (dif = NULL, gap = NULL)]

last_year <- gsub(".*_", "", fl)
last_year <- as.numeric(substr(last_year, 1 ,4)) - 1

annual <- forms[YEAR <= last_year]

### matching permno and cusip to annual file
comp <- fread("/Users/evolkova/Dropbox/DataY/Compustat/crsp_compustat_merger_annual.csv",
              select = c("cik", "LPERMNO", "cusip", "fyear", "fyr"))

crsp_monthly <- fread("/Users/evolkova/Dropbox/DataY/CRSP/MSF/CRSP_MSF.csv")
match <- match(as.numeric(annual$sbj_CIK), comp$cik)
annual$Permno <- comp$LPERMNO[match]
annual$cusip_comp <- comp$cusip[match]

require(stringr)
crsp_monthly <- crsp_monthly[!is.na(RET) & !is.na(PERMNO) & (str_length(COMNAM) > 2)]
crsp_monthly[, cusip6 := substr(CUSIP, 1, 6)]
match <- match(annual$CUSIP6, crsp_monthly$cusip6)
annual$Permno <- crsp_monthly$PERMNO[match]
annual$marcap <- crsp_monthly$SHROUT[match]*abs(crsp_monthly$PRC[match])

match <- match(substr(annual$cusip_comp[is.na(annual$Permno)],1,6), crsp_monthly$cusip6)
annual$Permno[is.na(annual$Permno)] <- crsp_monthly$PERMNO[match]

match <- match(annual$Permno, crsp_monthly$PERMNO)
annual$sbj_cname_crsp <- crsp_monthly$COMNAM[match]

### mark institutional investors
sec_master <- readRDS("/Users/evolkova/Dropbox/DataY/sec_master_13f_1994_2018.rds")
colnames(sec_master) <- c("cik", "name", "form_type", "date", "filename", "link")
setkey(annual, sbj_CIK, YEAR, fil_CIK)
sec_master <- sec_master[grep("13F",sec_master$form_type)]
sec_master[, year := year(date)]
sec_master[, cik_year := paste(cik, year)]
annual$files_13F <- 0
annual$files_13F[paste(as.numeric(annual$fil_CIK), annual$YEAR) %in% sec_master$cik_year] <- 1

### calculate statistics for aggregate ownership, HHI, # blocks, etc
annual[, `:=` (num_block = length(unique(fil_CIK)), block_hold = sum(max_prc)), by = c("sbj_CIK", "YEAR")]
annual[, block_portion := max_prc/block_hold]
### identity categories
annual[, `:=` (individual = 0, active_inst = 0, passive_inst = 0, other = 0)]
annual[item12 == "in", individual := 1]
annual[grepl("13D", TYPE) & files_13F == 1 & individual == 0, active_inst := 1]
annual[grepl("13G", TYPE) & files_13F == 1 & individual == 0, passive_inst := 1]
annual[, other := 1 - individual - active_inst - passive_inst]

annual[, `:=` (tmp1 = block_portion*individual, tmp2 = block_portion*active_inst, tmp3 = block_portion*passive_inst, tmp4 = block_portion*other)]
annual[, `:=` (HHI_group = sum(tmp1,na.rm=T)^2 + sum(tmp2,na.rm=T)^2 + sum(tmp3,na.rm=T)^2 + sum(tmp4,na.rm=T)^2), by = c("sbj_CIK", "YEAR")]
annual[, `:=` (diversity_identity = 1 - HHI_group), by = c("sbj_CIK", "YEAR")]
annual[, `:=` (tmp1 = NULL, tmp2 = NULL, tmp3 = NULL, tmp4 = NULL, HHI_group = NULL)]

### size categories
annual[, nstock_files := length(unique(sbj_CIK)), by = c("fil_CIK", "YEAR")]
annual[, `:=` (size1 = 0, size2 = 0, size3  = 0, size4 = 0)]
annual[nstock_files == 1, size1 := 1]
annual[nstock_files %in% 2:20, size2 := 1]
annual[nstock_files %in% 21:220, size3 := 1]
annual[nstock_files > 220, size4 := 1]

annual[, `:=` (tmp1 = block_portion*size1, tmp2 = block_portion*size2, tmp3 = block_portion*size3, tmp4 = block_portion*size4)]
annual[, `:=` (HHI_group = sum(tmp1,na.rm=T)^2 + sum(tmp2,na.rm=T)^2 + sum(tmp3,na.rm=T)^2 + sum(tmp4,na.rm=T)^2), by = c("sbj_CIK", "YEAR")]
annual[, `:=` (diversity_size = 1 - HHI_group), by = c("sbj_CIK", "YEAR")]
annual[, `:=` (tmp1 = NULL, tmp2 = NULL, tmp3 = NULL, tmp4 = NULL, HHI_group = NULL)]

### turnover categories
#fwrite(annual, "tmp.csv")
#annual <- fread("tmp.csv")
annual[ , stake_size := (max_prc-5)/100*marcap]
annual[max_prc < 5 , stake_size := 0]

setkey(annual, fil_CIK, YEAR)
annual[ , L.stake_size := c(NA, stake_size[-.N]), by = fil_CIK]
annual[ , delta.stake_size := stake_size - L.stake_size]
annual[is.na(delta.stake_size), delta.stake_size := 0]

annual[, top := sum(abs(delta.stake_size), na.rm = T), by = c("fil_CIK", "YEAR")]        
annual[, bottom := 1*sum(stake_size, na.rm = T) +  0*sum(L.stake_size, na.rm = T), 
       by = c("fil_CIK", "YEAR")] 
annual[, turn := top/bottom]

annual <- setDT(annual)[, turn_q := cut(turn, quantile(turn, probs=0:4/4, na.rm = T),
                                        include.lowest=TRUE, labels=FALSE), by = YEAR]


annual[, `:=` (turn1 = 0, turn2 = 0, turn3  = 0, turn4 = 0)]
annual[turn_q == 1, turn1 := 1]
annual[turn_q == 2, turn2 := 1]
annual[turn_q == 3, turn3 := 1]
annual[, turn4 := 1 - turn1 - turn2 - turn3]

annual[, `:=` (tmp1 = block_portion*turn1, tmp2 = block_portion*turn2, tmp3 = block_portion*turn3, tmp4 = block_portion*turn4)]
annual[, `:=` (HHI_group = sum(tmp1,na.rm=T)^2 + sum(tmp2,na.rm=T)^2 + sum(tmp3,na.rm=T)^2 + sum(tmp4,na.rm=T)^2), by = c("sbj_CIK", "YEAR")]
annual[, `:=` (diversity_turnover = 1 - HHI_group), by = c("sbj_CIK", "YEAR")]
annual[, `:=` (stake_size = NULL, L.stake_size = NULL, delta.stake_size = NULL,
               top = NULL, bottom = NULL, turn = NULL, turn_q = NULL,
               tmp1 = NULL, tmp2 = NULL, tmp3 = NULL, tmp4 = NULL, HHI_group = NULL)]

fwrite(annual, paste0(out_dir, "annual.csv"))
