###############################################################################
#################### Setting up ###############################################
###############################################################################
require(pacman)
p_load(data.table, lubridate, dplyr, stringr, Hmisc, httr, zoo, RCurl, DBI, RSQLite)


### folder with Parsed Forms
out_dir <- "/Users/evolkova/Dropbox/DataY/Blocks/Parsed Forms/"

start <- 1994
end <- 2021

start_date <- paste(start, "0101") %>% ymd %>% floor_date("quater")
end_date <- paste0(end, "1231") %>% ymd 
dates <- seq(start_date, end_date, by = "quarters")

###############################################################################
#################### Functions  ###############################################
###############################################################################

### extract lines around where to search for information
get.lines <- function(x) {
  if (is.na(x) | is.na(x)) {
    return(x)
  }
  y <- textConnection(x)
  body <- unlist(readLines(y))
  # body <- tolower(body)
  ### 98% of forms have length below 3000
  ### and I am looking for mentions of prc only in the first part
  body <- body[1:3000]
  body <- body[which(str_detect(body, "[:graph:]"))]

  ### we with 15 lines after each word "percent" and collapse them into one line
  ind <- grep("percent", body, ignore.case = T)
  lines <- NULL
  for (i in ind) lines <- c(lines, paste(body[(i):(i + 15)], collapse = " \n"))
  close(y)
  return(lines)
}
### extract positions of all investors
get.prc <- function(all_lines) {
  require(stringr)
  ### clean this lines from extra spaces
  all_lines <- unlist(all_lines)
  get.first.lines <- function(x, n) x <- paste(unlist(strsplit(x, "\n"))[1:n], collapse = " ")

  locate.prc <- function(lines) {
    lines <- tolower(lines)
    # at first we locate a line
    #  regex_find_line <- c("(?<=(row)).*(?=(type))",
    #                       "(?<=(row)).*(?=(page))",
    #                       "(?<=(row)).*(?=(cusip))",
    #                       "(?<=(owned)).*(?=(type))",
    #                       "(?<=(percent)).*(?=(type))")
    #  for(regex_fl in regex_find_line)
    #  {
    #     search.lines <- str_extract(lines, regex_fl)
    #    search.lines <- search.lines[grep("(\\d|n/a|none)",prc, perl = T)]
    #    if(length(prc) > 0) break
    #  }

    ### then search for percent expression
    regex_find_prc <- c(
      "(\\d{1,4}((\\,|\\.)\\d{0,7}|)( |)\\%|\\d{0,3}(\\.\\d{1,7}|)( |)\\%)",
      "-0-",
      "\\d{0,3}\\.\\d{1,7}", "\\d{0,3}\\.\\d{1,7}", "0  %"
    )

    for (regex_prc in regex_find_prc)
    {
      prc <- str_extract(lines, regex_prc)
      prc <- prc[!is.na(prc)]
      if (length(prc) > 0) break
    }
    return(prc)
  }

  for (end in 1:5 * 3) {
    lines <- lapply(all_lines, function(x) x <- get.first.lines(x, end))
    lines <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", lines, perl = TRUE)
    lines <- gsub("240.13", "", lines)

    lines <- gsub("-0-%", "0%", lines)
    ### here I have two spaces to find this pattern last
    lines <- gsub("none|n/a|less|-0-|lessthan5%", "0  %", lines)
    prc <- locate.prc(lines)
    if (length(prc) > 0) break
  }
  return(paste(prc, collapse = "|"))
}
### extract the maximum position in the block
### in almost all cases maximum position is
### a aggregate position among subsidiaries
get.max.prc <- function(x) {
  x <- gsub("%", "", x)
  x <- unlist(str_split(x, "\\|"))
  # x[grep("none|n/a|-0-|less", x)] <- 0
  x <- as.numeric(as.character(x))
  ### this 9 comes from row (9) in form in some filings

  ind <- which(x %/% 100 == 9)
  x[ind] <- x[ind] - 900
  ind <- which(x %/% 100 == 11)
  x[ind] <- x[ind] - 1100
  ind <- which(x %/% 10 == 11)
  x[ind] <- x[ind] - 110
  x <- x[x <= 100]
  return(max(x, na.rm = T))
}


for (i in 1:length(dates)) {
  print(Sys.time())
  print(paste0("year: ", year(dates[i]), ", quarter: ", quarter(dates[i])))
  dbname <- paste0(out_dir, "Clean Forms/sc13_", year(dates[i]), quarter(dates[i]), ".sqlite")

  ## connect to db
  con <- dbConnect(drv = RSQLite::SQLite(), dbname = dbname)
  ## Fetch data into data frame
  res <- dbSendQuery(con, "SELECT * FROM filings")
  res1 <- dbFetch(res, n = -1)

  sec_name <- paste0(out_dir, "Parsed Forms/", year(dates[i]), quarter(dates[i]), ".rds")
  sec_header <- readRDS(sec_name)
  match <- match(sec_header$FILENAME, res1$FILENAME)

  lines <- lapply(res1$FILING, get.lines)
  prc <- sapply(lines, get.prc)
  max.prc <- sapply(prc, get.max.prc)

  ### usually if information is missing here it is an exit filing
  ### information is missing in only ~1.5% of all forms
  max.prc[is.na(max.prc)] <- 0
  sec_header$max_prc <- max.prc[match]
  sec_header$prc <- prc[match] ### I keep in just in case
  saveRDS(sec_header, sec_name)
}
