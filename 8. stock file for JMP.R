dir_out <- "/Volumes/ORHAHOG_USB/Blocks/Working Files/"

require(data.table)
require(lubridate)

annual <- fread(paste0(dir_out, "annual.csv"))
load("./CRSP_COMP/crsp_monthly_1990_2016.rda")

###################################
### get all stock-year observations
###################################
max_year <- as.numeric(max(annual$YEAR))
tmp <- crsp_monthly[year(date) >= 1996 & year(date) <= max_year & month(date) == 12 & EXCHCD %in% 1:3]

stocks <- data.table(PERMNO = tmp$PERMNO, YEAR = year(tmp$date), CIK = NA, CNAME = tmp$COMNAM,
                     NCUSIP = tmp$NCUSIP, CUSIP = tmp$CUSIP, SIC = tmp$SICCD, PRC = abs(tmp$PRC),
                     SHROUT = tmp$SHROUT)
match <- match(stocks$PERMNO, annual$Permno)
stocks$CIK <- annual$sbj_CIK[match]
stocks[, marcap := PRC*SHROUT]
stocks <- stocks[!is.na(CIK) & !is.na(marcap)]

rm(crsp_monthly)
######################################
### collect information from compustat
######################################
load("./CRSP_COMP/CRSP_COMPUSTAT_1990_2016.rda")
comp <- as.data.frame(comp)
comp <- comp[comp$LPERMNO %in% stocks$PERMNO & !is.na(comp$at),]
comp <- as.data.table(comp)

na0 <- function(x) 
{
  x[is.na(x)] <- 0
  return(x)
}

comp[, year := fyear]
comp[fyr %in% 1:5, year := fyear + 1]
setkey(comp, LPERMNO, year)
match <- match(paste0(comp$LPERMNO, comp$year), paste0(tmp$PERMNO, year(tmp$date)))
comp$marcap <- tmp$SHROUT[match]*abs(tmp$PRC[match])/10^3
comp[, `:=` (at = at, size = log(at), growth = 100*(1 - sale/shift(sale, 1)), roa = ib/at, fcf = (ib + na0(dp))/at,
             tobin = (at - ceq + na0(txdb) + marcap)/at, fixed = na0(ppegt)/at, capex = na0(capxv)/at, leverage = (na0(dltt) + na0(dlc))/at), by = LPERMNO]

match <- match(paste(stocks$PERMNO, stocks$YEAR), paste(comp$LPERMNO, comp$year))
stocks$at <- comp$at[match]
stocks$size <- comp$size[match]
stocks$growth <- comp$growth[match]
stocks$roa <- comp$roa[match]
stocks$fcf <- comp$fcf[match]
stocks$tobin <- comp$tobin[match]
stocks$fixed <- comp$fixed[match]
stocks$capex <- comp$capex[match]
stocks$leverage <- comp$leverage[match]

match <- match(paste(stocks$PERMNO, stocks$YEAR + 1), paste(comp$LPERMNO, comp$year))
stocks$lead.at <- comp$at[match]
stocks$lead.size <- comp$size[match]
stocks$lead.growth <- comp$growth[match]
stocks$lead.roa <- comp$roa[match]
stocks$lead.fcf <- comp$fcf[match]
stocks$lead.tobin <- comp$tobin[match]
stocks$lead.fixed <- comp$fixed[match]
stocks$lead.capex <- comp$capex[match]
stocks$lead.leverage <- comp$leverage[match]

match <- match(paste(stocks$PERMNO, stocks$YEAR + 2), paste(comp$LPERMNO, comp$year))
stocks$lead2.at <- comp$at[match]
stocks$lead2.size <- comp$size[match]
stocks$lead2.growth <- comp$growth[match]
stocks$lead2.roa <- comp$roa[match]
stocks$lead2.fcf <- comp$fcf[match]
stocks$lead2.tobin <- comp$tobin[match]
stocks$lead2.fixed <- comp$fixed[match]
stocks$lead2.capex <- comp$capex[match]
stocks$lead2.leverage <- comp$leverage[match]

excl <- function(x)
{
  out <- is.na(x) | is.nan(x) | is.infinite(x)
  return(out)
}
ind <- (stocks$at) > 0 & !excl(stocks$size) & !excl(stocks$growth) & !excl(stocks$roa) & !excl(stocks$fcf) & !excl(stocks$tobin) & 
  !excl(stocks$fixed) & !excl(stocks$capex) & !excl(stocks$leverage) & !excl(stocks$SIC)
lead.ind <- (stocks$lead.at) > 0 & !excl(stocks$lead.size) & !excl(stocks$lead.growth) & !excl(stocks$lead.roa) & !excl(stocks$lead.fcf) & !excl(stocks$lead.tobin) & 
  !excl(stocks$lead.fixed) & !excl(stocks$lead.capex) & !excl(stocks$lead.leverage)


stocks <- stocks[ind & lead.ind]

###################################
### calculate amihud ##############
###################################
load(, file = "./CRSP_COMP/CRSP_daily_1990_2016.rda")
crsp_daily[,year := year(date)]
crsp_daily <- crsp_daily[year >= 1996]
for(cyear in 1996:2016)
{
  ### requesting WRDS
  print(cyear)
  start <- Sys.time()
  data <- crsp_daily[year == cyear]
  end <- Sys.time()
  print(end - start)
  
  ### computing amihud
  data <- as.data.table(data)
  setkey(data, PERMNO)
  data <- data[!is.na(RET) & !is.na(PRC) & !is.na(VOL) & PRC>0 & VOL>0]
  data[, RET := as.numeric(as.character(RET))]
  data[, amihud := 10^6*mean(abs(RET)/(VOL*PRC),na.rm = T), by = PERMNO]
  
  ### matching to our file
  match <- match(stocks$PERMNO[stocks$YEAR == cyear], data$PERMNO)
  stocks$amihud[stocks$YEAR == cyear] <- data$amihud[match]
}
        
###################################
#### get FF industries ############
###################################
match_FF <- function(sic_codes, file)
{
  
  #### make a table
  ffind <- readLines(file)
  ###ffind <- readLines("Siccodes12.txt")
  indtable <- NULL
  for(i in 1:length(ffind))
  {
    str <- ffind[i]
    if(str_length(str) == 0) next
    if (grepl("\\b(\\d|\\d\\d)\\b", str, perl = T))
    {
      res <- regexpr("\\b(\\d|\\d\\d)\\b", str, perl = T)
      ind <- regmatches(str, res)
    }
    else
    {
      res <- regexpr("\\b(?<year>\\d\\d\\d\\d)(?=-)", str, perl = T)
      begin <- regmatches(str, res)
      res <- regexpr("(?<=-)(?<year>\\d\\d\\d\\d)\\b", str, perl = T)
      end <- regmatches(str, res)
      indtable <- rbind(indtable, data.frame(begin = begin, end = end, ind = ind))
    }
  }
  table <- NULL
  table$sic <- as.numeric(as.character(sic_codes))
  table$FF <- NA
  table <- as.data.frame(table)
  
  ### actual matching
  for(i in 1:length(indtable$begin))
  {
    start <- as.numeric(as.character(indtable$begin[i]))
    end <- as.numeric(as.character(indtable$end[i]))
    index <- which(table$sic >= start & table$sic<= end)
    table$FF[index] <- indtable$ind[i]
  }
  return(table$FF)
}

stocks$FF_ind48 <- match_FF(stocks$SIC, "./CRSP_COMP/Siccodes48.txt")
stocks$FF_ind12 <- match_FF(stocks$SIC, "./CRSP_COMP/Siccodes12.txt")
stocks[, ind_year := paste(FF_ind48, YEAR)]
rm(crsp_daily)

###################################
#### inst ownership info  #########
###################################
load(, file = "./CRSP_COMP/inst_hold_1990_2015.rda")
inst_hold <- inst_hold[month(date) == 12]
setkey(inst_hold, cusip, date, mgrno)
inst_hold[, prc_own := 100*shares/shrout]
inst_hold <- inst_hold[!is.infinite(prc_own) & !is.na(prc_own)]
inst_hold[, inst_own := sum(prc_own, na.rm = T), by = c("date", "cusip")]
inst_hold[inst_own > 100, inst_own := 100]
inst_hold[, year := year(date)]

match <- match(paste(stocks$CUSIP, stocks$YEAR), paste(inst_hold$cusip, inst_hold$year))
stocks$inst_hold <- inst_hold$inst_own[match]
stocks$inst_hold[is.na(stocks$inst_hold)] <- 0
rm(inst_hold)
###################################
#### block ownership info #########
###################################

match <- match(paste(stocks$PERMNO, stocks$YEAR), paste(annual$Permno, annual$YEAR))
stocks[, `:=` (num_block = annual$num_block[match], block_hold = annual$block_hold[match], 
               diversity_identity = annual$diversity_identity[match], 
               diversity_size = annual$diversity_size[match],
               diversity_turnover = annual$diversity_turnover[match])]
stocks[is.na(match), `:=` (num_block = 0, block_hold = 0, diversity_identity = 0, 
               diversity_size = 0, diversity_turnover = 0)]

###################################
########### INSTRUMENTS  ##########
###################################

###################################
###### payout instrument ##########
###################################


comp[, payout_gross := sum(na0(dvc),na0(prstkc), na.rm = T), by = c("LPERMNO", "fyear")]
match <- match(paste0(annual$Permno, annual$YEAR), paste0(comp$LPERMNO, comp$year))
annual$payout_gross <- comp$payout_gross[match]
annual$div_gross <- comp$dvc[match]

annual[is.na(payout_gross), payout_gross := 0]
annual[is.na(div_gross), div_gross := 0]

annual[, payout_block := (max_prc/100*payout_gross/log(1 + nstock_files))]
annual[, div_block := (max_prc/100*div_gross/nstock_files)]

annual[, payout_stock := sum(payout_block, na.rm = T), by = c("sbj_CIK", "YEAR")]
annual[, payout_div_stock := sum(div_block, na.rm = T), by = c("sbj_CIK", "YEAR")]

annual[, payout_outside := payout_stock - payout_block]
annual[, payout_outside_div := payout_div_stock - div_block]

annual[, L.payout_outside := c(NA, payout_outside[-.N]), by = c("sbj_CIK")]
annual[, L.payout_outside_div := c(NA, payout_outside_div[-.N]), by = c("sbj_CIK")]

m <- match(paste(stocks$PERMNO, stocks$YEAR), paste(annual$Permno, annual$YEAR))
stocks[, `:=` (payout_outside =  annual$payout_outside[m], payout_outside_div = annual$payout_outside_div[m])]

stocks[is.na(payout_outside), payout_outside := 0]
stocks[is.na(payout_outside_div), payout_outside_div := 0]

stocks[, payout_outside_ta := payout_outside/marcap]
stocks[, payout_outside_div_ta := payout_outside_div/marcap]

###################################
###### merger instrument ##########
###################################
clean_phone <- function(x)
{
  x <- substr(as.numeric(gsub("[^0-9]", "", x)),1,10)
  return(x)
}
load(, file =  "./CRSP_COMP/MA/mergers.rda")
### find financial mergers
mergers[, `:=` (sic = as.numeric(as.character(Target.Primary...SIC..Code)), fin = 0)]
mergers[sic %/% 100 %in% c(60, 62, 61, 64, 65), fin := 1]
mergers[, fin := 1]
mergers <- mergers[fin == 1 & year %in% annual$YEAR]

### use phone number to match
mergers[, phone := clean_phone(Acquiror.Phone.Number)]
mergers <- mergers[!is.na(phone)]

annual[, phone := clean_phone(fil_business_address_phone)]
match <- match(paste(annual$phone, annual$YEAR), paste(mergers$phone, mergers$year))

### whether blockholder acquired a fin company
annual[, merger := 0]
annual[!is.na(match), merger := 1]
annual[nstock_files > 100, merger := 0]
annual[, fil_merger := max(merger), by = c("sbj_CIK", "YEAR")]

m <- match(paste(stocks$PERMNO, stocks$YEAR), paste(annual$Permno, annual$YEAR))
stocks$merger <- annual$fil_merger[m]
stocks[is.na(merger), merger := 0]

### load mergers from manual matching
### it is taking from a previous working file
add_mergers <- read.csv("/Volumes/ORHAHOG_USB/Blocks/Working Files/mergers_manual_match.csv")
add_mergers <- add_mergers[add_mergers$mergers == 1,]
stocks$merger[paste(stocks$PERMNO, stocks$YEAR) %in% paste(add_mergers$permno, add_mergers$year)] <- 1

win <- function(x, eps = 0.01)
{
  up <- quantile(x, na.rm = T, 1 - eps)
  down <- quantile(x, na.rm = T, eps)
  x[x>up] <- up
  x[x<down] <- down
  return(x)
}
stocks[, `:=` (lead.tobin = win(lead.tobin), inst_hold = win(inst_hold), growth = win(growth),
              size = win(size), fixed = win(fixed), capex = win(capex), leverage = win(leverage),
              amihud = win(amihud), block_hold = win(block_hold), num_block = win(num_block),
              diversity_identity = win(diversity_identity), diversity_size = win(diversity_size),
              diversity_turnover = win(diversity_turnover), payout_outside_ta = win(payout_outside_ta),
              payout_outside_div_ta = win(payout_outside_div_ta))]
require(lfe)

reg <- NULL
reg[[1]] <- felm(lead.tobin  ~ inst_hold + growth + size + fixed + capex + leverage + 
              amihud|PERMNO + ind_year|
              (block_hold + diversity_identity ~ merger + payout_outside_div_ta)|
              ind_year, data = stocks[num_block > 1])
reg[[2]] <- felm(lead.tobin  ~ inst_hold + growth + size + fixed + capex + leverage + 
                   amihud|PERMNO + ind_year|
                   (block_hold + diversity_size ~ merger + payout_outside_div_ta)|
                   ind_year, data = stocks[num_block > 1])
reg[[3]] <- felm(lead.tobin  ~ inst_hold + growth + size + fixed + capex + leverage + 
                   amihud|PERMNO + ind_year|
                   (block_hold + diversity_turnover ~ merger + payout_outside_div_ta)|
                   ind_year, data = stocks[num_block > 1])
require(stargazer)
stargazer(reg, type = "text")




reg <- felm(block_hold ~ merger +inst_hold + growth + size + fixed + capex + leverage + amihud  
            |PERMNO + ind_year|0|ind_year, data = stocks[num_block > 1])
summary(reg)

reg <- felm(diversity_identity ~payout_outside_ta + merger +inst_hold + growth + size + fixed + capex + leverage + amihud  
            |PERMNO + ind_year|0|ind_year, data = stocks[num_block > 1])
summary(reg)


hist(stocks$diversity_identity)
hist(stocks$diversity_size)
