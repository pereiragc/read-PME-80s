#' Generates table with year / state / file path from user settings
generatePathTables <- function(parameters) {
  lreadraw <- lapply(parameters$batch_subpaths, function(subpath) {
    list.files(file.path(parameters$base_data_path, subpath),
               recursive = TRUE, full.names = TRUE)
  })

  if (!is.null(lreadraw$pme80_90)) {
    ## Recover relevant files through .DAT extension

    paths_80_90 <- grep(".dat$", lreadraw$pme80_90,
                        ignore.case = TRUE, value = TRUE)

    ## Infer state and year via the "PME{year}{state}.DAT" format
    regex_80_90 <- ".*pme(\\d{2})(\\w{2})\\d?\\.dat"

    dt_paths_80_90 <-
      list(
        path = paths_80_90,
        year = paste0("19",
                      gsub(regex_80_90, "\\1",
                           paths_80_90, ignore.case = TRUE)),
        state = gsub(regex_80_90, "\\2", paths_80_90, ignore.case = TRUE)
      )
  }

  return(list(
    pme80_90 = data.table::as.data.table(dt_paths_80_90)
  ))
}

#' Uses path table generated from `generatePathTables` to read raw files
readYearRaw_80_90 <- function(yyyy, dt_path) {
  dt_raw <- dt_path[year == as.character(yyyy),
                    readRaw(path, year, state), path]
}


readRaw <- function(path, year, state) {
  dt <- data.table::fread(path, header = FALSE, sep = "$",
                          strip.white = FALSE)

  if (ncol(dt) > 1) {
    stop(glue::glue("[readRaw] {{year}} - {{state}} error: multiple colums"))
  }

  data.table::setnames(dt, 1, "full_line")
  dt[, `:=`(year = as.integer(year),
            path = path,
            state = state)]
}
