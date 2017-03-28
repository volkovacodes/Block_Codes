dir <- "/Volumes/ORHAHOG_USB/Blocks/Parsed Forms/"
dir_out <- "/Volumes/ORHAHOG_USB/Blocks/Working Files/"

require(data.table)
require(lubridate)
### reading all forms
files <- list.files(dir)
forms <- NULL
for(fl in files) forms <- rbind(forms, fread(paste0(dir, fl)))

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
add_gaps <- function(forms, step)
{
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

forms <- forms[YEAR <= last_year]
write.csv(forms, "annual.csv", row.names = F)
