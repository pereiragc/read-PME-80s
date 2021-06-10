#!/usr/bin/env Rscript

library(data.table)
library(optparse)

option_list <- list(
  make_option(c("-i", "--input"), type = "character",
              help = "Path to unzipped and pre-treated PME 1982-1990 datasets.",
              metavar = "character"),
  make_option(c("-o", "--out"), type = "character", default = "./output",
              help = "output directory [default= %default]",
              metavar = "character"),
  make_option(c("-t", "--trimws"), action = "store_true", type = "logical",
              default = FALSE,
              help = "Trim whitespace in all parsed fields. Significantly slows down execution"),
  make_option(c("--yrange"), type = "character", default = "all",
              help = "One of 'all' or a range of years, e.g., --yrange=1985-1986. [default=%default]",
              metavar = "[YYYY-YYYY|all]"),
  make_option(c("--yexclude"), type = "character", default = "",
              help = "Years to exclude. Defaults to empty string. (If set, do not use spaces.)",
              metavar = "YYYY,...,YYYY"),
  make_option(c("-s", "--save_parsed_dictionaries"), type = "logical", default = FALSE, action = "store_true",
              help = "Should output include parsed dictionaries? (Potentially useful for list of available fields.) [default=%default]")
)

opt_parser <- OptionParser(option_list = option_list,
                           description = "Read raw pme data and convert them to csv files by year. Works for any year in the 1984 - 1990 range.",
                           epilogue = "Example usage: \n$ ./pmeread.r --yrange=1984-1985\n  Converts years 1984 and 1985.\n$ ./pmeread.r --yexclude=1988\n  Converts all available years except 1988.");
opt <- parse_args(opt_parser);



if (is.null(opt$input)) {
  stop("Please specify input directory with `-i DIR`.")
} else {
  if (!dir.exists(opt$input)) stop("Specified input directory was not found")
}

## Initial checks
if (!dir.exists(opt$out)) stop(glue::glue("Directory `{opt$out}` does not exist; please create it."))
if (file.access(opt$out, 2) != 0) stop(glue::glue("Directory `{opt$out}` is not writable, please check your permissions."))


## Parse year range
target_years <- c(1984, 1990)
if (!(opt$yrange == "all")) {
  yrange_regex <- "(\\d{4})-(\\d{4})"
  if (!grepl(yrange_regex, opt$yrange)) stop("Incorrectly specified `--yrange`.")

  y0 <- gsub(yrange_regex, "\\1", opt$yrange)
  y1 <- gsub(yrange_regex, "\\2", opt$yrange)

  y0n <- min(as.integer(y0), as.integer(y1))
  y1n <- max(as.integer(y0), as.integer(y1))

  if (y0n < target_years[1]) stop(glue::glue("Year {y0n} not available."))
  if (y1n > target_years[2]) stop(glue::glue("Year {y1n} not available."))

  target_years <- c(y0n, y1n)
}

exclude_years <- integer(0)
if (nchar(opt$yexclude) > 0) {
  if (nchar(opt$yexclude) > 100) stop("No way")

  sepstr <- strsplit(opt$yexclude, ",", fixed= TRUE)[[1]]
  if (!all(grepl("\\d{4}", sepstr))) stop("Invalid years supplied to `--yexclude`.")

  exclude_years <- as.integer(sepstr)
  if (!all(data.table::inrange(exclude_years, target_years[1], target_years[2]))) warning("Redundant years supplied to `--yexclude`")
}

target_years <- setdiff(seq(target_years[1], target_years[2], by = 1),
                        exclude_years)

target_years_str <- paste(target_years, collapse=", ")

message(glue::glue("Converting following years: {target_years_str}"))
message(glue::glue("Trim whitespace? {opt$trimws}"))

## end of flags / options section ----------------------------------------------


## Heavy lifting is done "lib.r"
source("lib.r", chdir = TRUE)
source("helper_functions.r", chdir = TRUE)

.internal_params <- list(
  dict_path = "data/dictionaries",
  hhlength = 40L,
  personlength = 110L,
  useParallel = TRUE,
  cores_override = 8L,
  nblocks = 50L
)


dt_path <- generatePathTables(opt$input)

ws_handle <- if(opt$trimws) function(x) trimws(x)[1] else function(x) identity(x)

for (yyyy in  target_years) {
  message(glue::glue("[Year {yyyy}] Starting job"))


  message(glue::glue("[Year {yyyy}] Parsing dictionary... "))
  dt_col <- readDict(yyyy, .internal_params)
  message(glue::glue("[Year {yyyy}] Done. "))

  message(glue::glue("[Year {yyyy}] Reading raw PME data... "))
  t0 <- Sys.time()
  dt_raw <- readYearRaw(yyyy, dt_path, .internal_params)
  t0 <- Sys.time() - t0
  n <- dt_raw[, .N]
  message(glue::glue("[Year {yyyy}] Done reading {n} rows"))

  message(glue::glue("[Year {yyyy}] Converting... "))
  col_breakdown <- list(
    hhcols = dt_col[End <= .internal_params$hhlength],
    perscols = dt_col[End > .internal_params$hhlength]
  )


  lconv <- convertRawData(dt_raw, col_breakdown, .internal_params)
  dt_long <- lconv$dtout

  print(colnames(dt_long))


  message(glue::glue("[Year {yyyy}] Done"))

  message(glue::glue("[Year {yyyy}] Reshaping data into wide format..."))

  t0 <- Sys.time()
  person <- dcast(dt_long[type == 0], n_entry + person_id + .state ~ Name,
                  fun.aggregate = ws_handle,
                  value.var = "val",
                  fill = NA)
  t0 <- Sys.time() - t0
  message(glue::glue("     cast person dataset"))

  t1 <- Sys.time()
  hh <- dcast(dt_long[type == 1], n_entry + person_id + .state ~ Name,
                  fun.aggregate = ws_handle,
                  value.var = "val",
                  fill = NA)

  t1 <- Sys.time() - t1
  message(glue::glue("     cast household dataset"))
  message(glue::glue("[Year {yyyy}] Done."))



  message(glue::glue("[Year {yyyy}] Saving output..."))
  fperson <- file.path(opt$out, glue::glue("person-{yyyy}.csv"))
  fhh <- file.path(opt$out, glue::glue("household-{yyyy}.csv"))

  fwrite(person, fperson)
  fwrite(hh, fhh)
  if (opt$save_parsed_dictionaries) fwrite(dt_col, file.path(opt$out,
                                                             glue::glue("dict-{yyyy}.csv")))
  message(glue::glue("[Year {yyyy}] Done."))
}
