#' Generates table with year / state / file path from user settings
generatePathTables <- function(parameters) {
  lreadraw <- list.files(file.path(parameters$base_data_path),
                         recursive = TRUE, full.names = TRUE)

  ## Recover relevant files through .DAT extension
  paths <- grep(".dat$", lreadraw,
                      ignore.case = TRUE, value = TRUE)

  ## Infer state and year via the "PME{year}{state}.DAT" format
  fname_regex <- ".*pme(\\d{2})(\\w{2})\\d?\\.dat"

  dt_paths <-
    list(
      fname = paths,
      year = paste0("19",
                    gsub(fname_regex, "\\1",
                         paths, ignore.case = TRUE)),
      state = gsub(fname_regex, "\\2", paths, ignore.case = TRUE)
    )

  return(data.table::as.data.table(dt_paths))
}

#' Uses path table generated from `generatePathTables` to read raw files
readYearRaw <- function(yyyy, dt_path, .internal_params) {
  dt_raw <- dt_path[year == as.character(yyyy),
                    readRaw(fname, year, state, .internal_params), fname]
}


readRaw <- function(fname, year, state, .internal_params) {
  dt <- data.table::fread(fname, header = FALSE, sep = "$",
                          strip.white = FALSE)

  if (ncol(dt) > 1) {
    stop(glue::glue("[readRaw] {{year}} - {{state}} error: multiple colums"))
  }

  data.table::setnames(dt, 1, "full_line")
  dt[, `:=`(year = as.integer(year),
            fname = fname,
            state = state)]
}



readDict <- function(yyyy, parameters, .internal_params) {
  lf <- list.files(.internal_params$dict_path,
                   pattern = as.character(yyyy),
                   full.names = TRUE)
  if (length(lf) != 1) stop("[readDict] Ambiguous dictionary specification")

  dict <- parseDictionary(yyyy, lf, parameters)
  dict[]
}

parseDictionary <- function(yyyy, fname, parameters) {
  dt_input <- unique(data.table::fread(fname, header = FALSE,
                                       encoding = "Latin-1"))


  cnames_hint <- c("VARI", "NOME", "DESDE", "TAM")
                                        # ^ These names should be present on ALL
                                        # years

  success <- setCorrectNames(dt_input, cnames_hint)
                                        # changes in place!! (& returns a
                                        # code indicating success)
  if (!success) {
    ## TODO: log
    return(NULL)
  }


  ## Tidy up
  cnames_tidy <- c("Name", "Descr", "Pos", "Width")
  data.table::setnames(dt_input, cnames_hint, cnames_tidy)
  dt_input <- dt_input[, .(Name, Descr, Pos, Width)]


  ## Get relevant entries
  dt_input <- dt_input[grepl("[0-9]+", Name)]

  ## Append "V" in front of variable names if needed
  dt_input[grepl("^[^V]", Name), Name := paste("V", Name, sep="")]


  ## Remove duplicate names (keep first)
  dt_input[, n_appear := seq_len(.N), Name]
  dt_input <- dt_input[n_appear == 1]
  dt_input[, n_appear := NULL]


  ## Compute Start/End positions
  dt_input[, `:=`(Pos = as.integer(Pos),
                  Width = as.integer(Width))]
  dt_input[, `:=`(Start = Pos, End = Pos + Width - 1)]

  if (parameters$save_parsed_dictionaries) {
    data.table::fwrite(dt_input,
                       glue("./output/treated_{basename(fname)}"))
  }
  return(dt_input)
}



##' Corrects names in
setCorrectNames <- function(dt_input, cnames_hint) {
  ## Context: the header of `dt_input` is in some of the rows of `dt_input`.
  ##
  ## I know the header *must* contain the column names in `cnames_hint`. So
  ## the idea of this function is:
  ##
  ## - sweep rows of `dt_input` trying to find which one contains all the column
  ##   names in `cnames_hint` (this is the `apply` step below)
  ##
  ## - there might be no row containing `cnames_hint` (in which case the
  ##   dictionary file is considered defectuous, and the function returns a
  ##   "bad" code indicating failure)
  ##
  ## - there might be multiple rows containing `cnames_hint`. I pick the first
  ##   one. Names outside `cnames_hint` may be blank, for example, in which case
  ##   that would not be a valid header. What I do in that case is to only
  ##   change the non-blank column names of `dt_input`.
  ##
  ##   There are obvious heuristics that would implement the same goal more
  ##   robustly. Since it's not useful for my purposes, I leave this for future
  ##   PRs :D


  find_rows <- which(apply(dt_input, 1, function(x) all(cnames_hint %in% x)))

  if (length(find_rows) == 0) return(FALSE) # return bad code


  candidate_header <- unlist(dt_input[find_rows[1]])


  ## "Non-empty" column names (by definition are a superset of `cnames_hint`)
  indices_nonempty <-
    Filter(function(i) nchar(trimws(candidate_header[i])) > 0,
           seq_along(candidate_header))


  setnames(dt_input, colnames(dt_input)[indices_nonempty],
           candidate_header[indices_nonempty])

  return(TRUE)
}


pmeProcessLine <- function(pmeline, dt_col, n_entry, fname,
                           col_breakdown, .internal_params) {
  hhlength <- .internal_params$hhlength
  personlength <- .internal_params$personlength


  ## Deal with person substring
  npersons <- countPersons(pmeline, .internal_params)

  persons <- rbindlist(lapply(seq_len(npersons), function(i) {
    ch1 <- hhlength + (i - 1) * personlength + 1
    ch2 <- ch1 + personlength - 1
    personstr <- substr(pmeline, ch1, ch2)
    mapPerson(personstr, n_entry, i, col_breakdown, .internal_params)
  }))

  if (!nrow(persons) == 0) persons[, type := 0]


  ## Deal with household substring
  hhstr <- substr(pmeline, 1, hhlength)
  hh <- mapHousehold(hhstr, n_entry, col_breakdown)

  hh[, `:=`(
    type = 1,
    person_id = NA_integer_)] # for binding

  return(rbind(hh, persons))
}

mapPerson <- function(personstr, n_entry, i, col_breakdown, .internal_params) {
  hhlength <- .internal_params$hhlength

  col_breakdown$perscols[, {
    list(n_entry = n_entry,
         person_id = i,
         val = substr(personstr,
                      Start[1] - hhlength,
                      End[1] - hhlength))
  }, Name]
}

mapHousehold <- function(hhstr, n_entry, col_breakdown) {

  col_breakdown$hhcols[, {
    list(n_entry = n_entry,
         val = substr(hhstr, Start[1], End[1]))
  }, Name]

}



countPersons <- function(pmeline, .internal_params) {
  hhlength <- .internal_params$hhlength
  personlength <- .internal_params$personlength

  npersons <- (nchar(pmeline) - hhlength) %/% personlength

  return(max(npersons, 0L))
}
