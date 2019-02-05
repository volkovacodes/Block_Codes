dir_out <- "/Volumes/KINGSTON/Blocks/Working Files/"

require(data.table)
require(lubridate)

annual <- fread(paste0(dir_out, "annual.csv"))
crsp_monthly <- fread("/Users/evolkova/Yandex.Disk.localized/CRSP/MSF/CRSP_MSF.csv",
                      select = c("date", "EXCHCD", "PERMNO", "COMNAM", "NCUSIP", "CUSIP",
                                 "PRC","SHROUT", "SICCD"))

###################################
### get all stock-year observations
###################################
max_year <- as.numeric(max(annual$YEAR))
crsp_monthly[, date := ymd(date)]
tmp <- crsp_monthly[year(date) >= 1994 & year(date) <= max_year & month(date) == 12 & EXCHCD %in% 1:3]

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
comp <- fread("/Users/evolkova/Yandex.Disk.localized/Compustat/crsp_compustat_merger_annual.csv")
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
for(cyear in 1994:max_year)
{
  ### requesting WRDS
  print(cyear)
  start <- Sys.time()
  data <- fread(paste0("/Users/evolkova/Yandex.Disk.localized/CRSP/DSF/CRSP_DSF_",cyear,".csv"),
                select = c("PERMNO", "RET", "PRC", "VOL", "date"))
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
stocks[is.na(FF_ind48), FF_ind48 := 48]
stocks[is.na(FF_ind12), FF_ind12 := 12]
stocks[, ind_year := paste(FF_ind48, YEAR)]

###################################
#### inst ownership info  #########
###################################
inst_hold <- fread("/Users/evolkova/Yandex.Disk.localized/13F_1990_2017.csv")
inst_hold <- inst_hold[month == 12]


match <- match(paste(stocks$PERMNO, stocks$YEAR), paste(inst_hold$PERMNO, inst_hold$year))
stocks$inst_hold <- inst_hold$InstOwnPrc[match]
stocks$inst_hold[is.na(stocks$inst_hold)] <- 0
stocks$inst_hold[(stocks$inst_hold) > 1] <- 1.0
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


win <- function(x, eps = 0.005)
{
  up <- quantile(x, na.rm = T, 1 - eps)
  down <- quantile(x, na.rm = T, eps)
  x[x>up] <- up
  x[x<down] <- down
  return(x)
}
stocks[, `:=` (lead.tobin = win(lead.tobin), inst_hold = win(inst_hold), 
               growth = win(growth),
               size = win(size), fixed = win(fixed), capex = win(capex), leverage = win(leverage),
               amihud = win(amihud), block_hold = win(block_hold), num_block = win(num_block),
               diversity_identity = win(diversity_identity), diversity_size = win(diversity_size),
               diversity_turnover = win(diversity_turnover))]
require(lfe)




reg <- NULL
reg[[1]] <- felm(lead.tobin  ~ inst_hold + growth + size + fixed + capex + leverage + 
                   amihud + block_hold + diversity_identity|PERMNO + ind_year|
                   0|ind_year, data = stocks[num_block > 1])
reg[[2]] <- felm(lead.tobin  ~ inst_hold + growth + size + fixed + capex + leverage + 
                   amihud + block_hold + diversity_size|PERMNO + ind_year|
                   0|ind_year, data = stocks[num_block > 1])
reg[[3]] <- felm(lead.tobin  ~ inst_hold + growth + size + fixed + capex + leverage + 
                   amihud + block_hold + diversity_turnover |PERMNO + ind_year|
                   0|ind_year, data = stocks[num_block > 1])
require(stargazer)
stargazer(reg, type = "text")

