dir_in <- "/Volumes/KINGSTON/Blocks/Clean Forms/"
dir_out <- "/Volumes/KINGSTON/Blocks/Parsed Forms/"
start_year <- 1994
start_QTR <- 1

end_year <- 2018
end_QTR <- 4
require(RSQLite)
require(data.table)
### generate sequence of quaters 
get_dates <- function(start_year, start_QTR, end_year, end_QTR)
{
  require(data.table)
  all_dates <- data.table(year = rep(1993:2050, 4))
  setkey(all_dates,year)
  all_dates[, QTR := 1:.N, by = year]
  all_dates <- as.data.frame(all_dates)
  
  x <- paste0(all_dates$year, all_dates$QTR) >= paste0(start_year, start_QTR) & paste0(all_dates$year, all_dates$QTR) <= paste0(end_year, end_QTR)
  return(all_dates[x,])
}
### list of all identities codes
code_list <- c("bd", "bk", "ic", "iv", "ia", "ep", "hc", "sa", "cp", "co", "pn", "in", "fi", "oo")
### I general regex to capture all variation of identity codes
gen_codes_regex <- function()
{
  codes <- NULL
  codes[[1]] <- c("broker\\s+dealer", "bd")
  codes[[2]] <- c("bank", "bk")
  codes[[3]] <- c("insurance\\s+company", "ic")
  codes[[4]] <- c("investment\\s+company", "iv")
  codes[[5]] <- c("investment\\s+advisor", "ia")
  codes[[6]] <- c("employee\\s+benefit", "ep")
  codes[[7]] <- c("holding\\s+company", "hc")
  codes[[8]] <- c("savings\\s+association", "sa")
  codes[[9]] <- c("church\\s+plan", "cp")
  codes[[10]] <- c("corporation", "co\\s", "c0") ### company sometimes is catched as corporation
  codes[[11]] <- c("partnership", "pn")
  codes[[12]] <- c("individual", "in", "in")
  codes[[13]] <- c("non-U.S.\\s+institution", "fi")
  codes[[14]] <- c("other", "oo", "o0", "00", "0.0", "o.o", "o.0", "0.o")
  
  for(i in 1:14)
  {
    codes[[i]] <- c(codes[[i]], 
                    paste0(code_list[i], 2), # 2 is for second part of the form, sometimes it mergers with the previous answer
                    paste0(code_list[i], "page"), # the same with the word page
                    paste0("person",code_list[i])) # sometimes there are no space before the word person
  }
  regex_codes <- lapply(codes, function(x)paste0("(\\b", x, "\\b)", collapse = "|"))
  return(regex_codes)
}  
regex_codes <- gen_codes_regex()
### this function looks for a line with phare "type of reporting person"
### and returns type codes from the next lines
get_phares <- function(text)
{
  require(stringr)
  text <- tolower(text)
  text <- unlist(strsplit(text, "\n"))
  text <- text[which(str_detect(text,"[:graph:]"))]
  ind <- grep("type\\s+(of|in|or)\\s+reporting\\s+person", text, perl = T)
  if(length(ind) == 0) return(NA)
  item12 <- NA
  ### first I check the next line, than line + 1 etc. up to line + 5
  for(i in 0:5)
  {
    #print(i)
    lines <- text[ind + i]
    vector <- sapply(regex_codes, function(x) max(grepl(x, lines, perl = T)))
    if(sum(vector) == 0) next
    #print(vector)
    item12 <- paste(code_list[which((vector) == 1)],collapse = "|")
    return(item12)
  }
  return(item12)
}
### sometimes phares "type of reporting person" is splitted across several lines
### I merge everything into one line and search for a phrase here 
### (this function is significantly slower than the previous one)
get_phares_one_line <- function(text)
{
  require(stringr)
  text <- tolower(text)
  text <- unlist(strsplit(text, "\n"))
  text <- text[which(str_detect(text,"[:graph:]"))]
  ### some forms could be too long, 
  ### so I look only at the first 3000 lines
  text <- text[1:3000]
  text <- paste(text, collapse = " ")
  text <- trimws(text)
  text <- gsub("\\s+", " ", text, perl = T)
  #ind <- grep("type\\s+(of|in|or)\\s+reporting\\s+person", text, perl = T)
  loc <- as.data.frame(str_locate_all(text, "type\\s(of|in|or)\\sreporting\\sperson"))
  if(length(loc$start) == 0) loc <- as.data.frame(str_locate_all(text, "type\\s(of|in|or)\\sperson\\sreporting"))
  
  if(length(loc$start) == 0) return(NA)
  item12 <- NA
  for(str_len in c(20, 40, 60, 80, 100, 120, 160))
  {
    #print(i)
    lines <- NULL
    for(start in loc$start)
    {
      lines <- c(lines, substr(text, start, start + str_len))
    }
    
    vector <- sapply(regex_codes, function(x) max(grepl(x, lines, perl = T)))
    if(sum(vector) == 0) next
    #print(vector)
    item12 <- paste(code_list[which((vector) == 1)],collapse = "|")
    return(item12)
  }
  return(item12)
}


dates <- get_dates(start_year, start_QTR, end_year, end_QTR)
dates$year_QTR <- paste0(dates$year, dates$QTR)
for(yearqtr in dates$year_QTR)
{
  print(Sys.time())
  print(yearqtr)
  
  ### read master file
  sec_name <- paste0(dir_out, "Parsed_forms_", yearqtr, ".rds")
  sec_header <- readRDS(sec_name)
  sec_header <- as.data.table(sec_header)
  ### read filings
  dbname <- paste0(dir_in, "sc13_", yearqtr, ".sqlite")
  con <- dbConnect(drv=RSQLite::SQLite(), dbname=dbname)
  ## Fetch data into data frame
  res <- dbSendQuery(con, "SELECT FILENAME, FILING FROM filings")
  res1 <- dbFetch(res,n=-1)
  
  ### check the order just in case
  match <- match(res1$FILENAME, sec_header$FILENAME)
  sec_header <- sec_header[match]
  
  ### call identity item12
  sec_header$item12 <- NA
  ### fidelity has its own formatting
  sec_header$item12[sec_header$fil_CIK == "0000315066"] <- "hc|in"
  ### the match a majority with the first faster function
  sec_header$item12[is.na(sec_header$item12)] <- sapply(res1$FILING[is.na(sec_header$item12)], get_phares)
  ### the rest is match with more diligent but slower function
  sec_header$item12[is.na(sec_header$item12)] <- sapply(res1$FILING[is.na(sec_header$item12)], get_phares_one_line)
  print(mean(is.na(sec_header$item12)))
  saveRDS(sec_header, sec_name)
}
