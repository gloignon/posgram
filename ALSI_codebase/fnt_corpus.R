# This script contains functions for processing and analyzing text data.
library(data.table)
library(tidyverse)
library(utf8)
library(udpipe)

constituerCorpus <- function(dossier, verbose = FALSE, clean = TRUE) {
  if (dir.exists(dossier)) {
    chemins <- list.files(path = dossier, full.names = TRUE)
    if (verbose) message("constituerCorpus | C'est un dossier!")
  } else if (file_test("-f", dossier)) {
    chemins <- normalizePath(dossier)
  } else {
    stop("constituerCorpus | dossier ou fichier inexistant")
  }
  
  nettoyerTexte <- function(chemin) {
    if (verbose) message("working on ", chemin)
    contenu <- read_file(chemin)
    contenu <- utf8_normalize(contenu, map_quote = TRUE)
    
    # Remove backslashes
    contenu <- gsub("\\\\", "", contenu)
    
    # Preserve true paragraph breaks (double line breaks), only remove single ones
    contenu <- gsub("\r?\n(?=[^\r\n])", " ", contenu, perl = TRUE)  # single line breaks → space
    contenu <- gsub("(\\r?\\n)\\s+", "\\1", contenu)                # remove space at beginning of line
    contenu <- str_replace_all(contenu, "\r", "")                   # normalize returns
    contenu <- str_replace_all(contenu, "\n{3,}", "\n\n")           # max two line breaks
    
    # Clean punctuation
    contenu <- str_replace_all(contenu, "’", "'")
    contenu <- str_replace_all(contenu, "''", "'")
    contenu <- str_replace_all(contenu, "«", " « ")
    contenu <- str_replace_all(contenu, "»", " » ")
    contenu <- str_replace_all(contenu, "!", "! ")
    contenu <- str_replace_all(contenu, ";", " ; ")
    contenu <- str_replace_all(contenu, ":", " : ")
    contenu <- gsub("\\.(?=[A-Za-zÀÉÈÙ0-9])", ". ", contenu, perl = TRUE)
    contenu <- gsub("\\,(?=[A-Za-zÀÉÈÙ0-9])", ", ", contenu, perl = TRUE)
    contenu <- gsub("\\)(?=[A-Za-zÀÉÈÙ0-9])", ") ", contenu, perl = TRUE)
    
    # Clean extra spaces
    contenu <- str_replace_all(contenu, " {3,}", "  ")
    contenu <- str_replace_all(contenu, " {2}", " ")
    
    return(contenu)
  }
  
  # Create corpus
  dt_corpus <- data.table(
    doc_id = basename(chemins),
    text = if (clean) unlist(lapply(chemins, nettoyerTexte)) else unlist(lapply(chemins, function(p) {
      utf8_normalize(read_file(p), map_quote = TRUE)
    }))
  )
  
  return(dt_corpus)
}

# Fast corpus builder for many .txt files
# - Parallel by default (future.apply) and zero per-file rbinds
# - Reads whole file in one call (readr::read_file or fast base::readChar fallback)
# - Uses data.table everywhere; single rbindlist at the end
# - Optional cleaning that preserves paragraph breaks by default

constituer_corpus_fast <- function(
    dir_or_files,
    pattern = "\\.txt$",
    recursive = TRUE,
    encoding = "UTF-8",
    clean = FALSE,
    keep_paragraphs = TRUE,
    parallel = TRUE,
    n_workers = max(1, parallel::detectCores() - 1),
    use_readr = TRUE,
    verbose = TRUE
) {
  requireNamespace("data.table")
  if (use_readr) requireNamespace("readr")
  requireNamespace("stringi")
  requireNamespace("fs")
  if (parallel) { requireNamespace("future"); requireNamespace("future.apply") }
  
  # ---- list files (robust) ----
  paths <- as.character(dir_or_files)
  exists_mask <- fs::file_exists(paths)
  if (!all(exists_mask)) {
    stop(sprintf("These paths do not exist:\n- %s",
                 paste(paths[!exists_mask], collapse = "\n- ")))
  }
  info <- fs::file_info(paths)
  dirs  <- paths[info$type == "directory"]
  files_explicit <- paths[info$type == "file"]
  
  files_from_dirs <- character(0)
  if (length(dirs)) {
    files_from_dirs <- fs::dir_ls(
      dirs, recurse = recursive, type = "file"
    )
  }
  
  files <- unique(c(files_explicit, files_from_dirs))
  if (length(files) == 0) stop("No files found in provided paths.")
  
  # apply pattern AFTER full expansion (e.g., keep only .txt)
  if (!is.null(pattern) && nzchar(pattern)) {
    files <- files[grepl(pattern, fs::path_file(files), perl = TRUE, ignore.case = TRUE)]
  }
  # double-check file-ness
  files <- files[fs::file_exists(files) & fs::is_file(files)]
  
  if (!length(files)) stop("No matching files after filtering (check `pattern`).")
  
  # If only one file, parallel adds overhead: turn it off
  if (length(files) == 1L) { parallel <- FALSE; if (verbose) cat("Only 1 file → disabling parallel.\n") }
  
  if (verbose) {
    cat(sprintf("Found %d files. Parallel: %s (%d workers)\n",
                length(files), parallel, if (parallel) n_workers else 1))
  }
  
  # ---- reader helpers ----
  read_one <- if (use_readr) {
    function(path, enc) readr::read_file(path, locale = readr::locale(encoding = enc))
  } else {
    function(path, enc) {
      sz  <- file.info(path, extra_cols = FALSE)$size
      con <- file(path, open = "rb")
      on.exit(close(con), add = TRUE)
      raw_txt <- readChar(con, nchars = sz, useBytes = TRUE)
      if (!is.na(enc)) raw_txt <- iconv(raw_txt, from = "", to = enc, sub = "byte")
      raw_txt
    }
  }
  
  clean_text <- function(x) {
    x <- stringi::stri_replace_all_fixed(x, "\r\n", "\n")
    x <- stringi::stri_replace_all_fixed(x, "\r", "\n")
    if (keep_paragraphs) {
      x <- stringi::stri_replace_all_regex(x, "[\t\f\v]+", " ")
      lines <- unlist(strsplit(x, "\n", fixed = TRUE), use.names = FALSE)
      lines <- stringi::stri_trim_both(lines)
      lines <- stringi::stri_replace_all_regex(lines, " {2,}", " ")
      x <- paste(lines, collapse = "\n")
      x <- stringi::stri_replace_all_regex(x, "\n{3,}", "\n\n")
    } else {
      x <- stringi::stri_replace_all_regex(x, "\\s+", " ")
      x <- stringi::stri_trim_both(x)
    }
    x
  }
  
  worker <- function(p) {
    # tryCatch to skip unreadable files gracefully
    out <- try({
      txt <- read_one(p, encoding)
      if (clean) txt <- clean_text(txt)
      fi  <- file.info(p, extra_cols = FALSE)
      data.table::data.table(
        doc_id = fs::path_ext_remove(fs::path_file(p)),
        text   = txt,
        path   = p,
        bytes  = as.double(fi$size),
        mtime  = fi$mtime
      )
    }, silent = TRUE)
    if (inherits(out, "try-error")) {
      warning(sprintf("Skipping unreadable file: %s\n  Reason: %s", p, as.character(out)))
      return(NULL)
    }
    out
  }
  
  if (parallel && length(files) > 1L) {
    oplan <- future::plan()
    on.exit(future::plan(oplan), add = TRUE)
    future::plan(future::multisession, workers = n_workers)
    chunks <- future.apply::future_lapply(
      files, worker, future.chunk.size = ceiling(length(files)/(n_workers*4))
    )
  } else {
    chunks <- lapply(files, worker)
  }
  
  chunks <- Filter(Negate(is.null), chunks)
  if (!length(chunks)) stop("All files failed to read.")
  dt <- data.table::rbindlist(chunks, use.names = TRUE, fill = TRUE)
  data.table::setDT(dt)
  if (verbose) {
    cat(sprintf("Built corpus: %d docs, %.1f MB total.\n",
                nrow(dt), sum(dt$bytes, na.rm = TRUE)/1024^2))
  }
  dt[]
}




parserTexte <- function(txt, ud_model = "models/french_gsd-remix_2.udpipe", nCores = 1) {
  
  # Check if the model file exists
  if (!file.exists(ud_model)) {
    stop(paste("UDPipe model not found at:", ud_model))
  }
  
  # Try to load the model safely
  model <- tryCatch({
    udpipe::udpipe_load_model(file = ud_model)
  }, error = function(e) {
    stop("Failed to load UDPipe model: ", e$message)
  })
  
  parsed <- udpipe(x = txt, object = model, trace = TRUE, parallel.cores = nCores)
  parsed <- as.data.table(parsed)
  return(parsed)
}


#' Post treatment on the output of a udpipe parsing
#'
#' @param dt A data.table containing the output of a udpipe parsing.

#' @return A data.table containing the edited output of the udpipe parsing.
#' @import data.table
postTraitementLexique <- function(dt) {
  cat("Dans PostTraitement lexique\n")
  
  parsed.post <- setDT(copy(dt))
  
  cat("Class of parsed.post: ", class(parsed.post), "\n")

  
  # Add a unique token ID
  parsed.post[, vrai_token_id := 1:.N]
  
  # Clean document IDs
  parsed.post[, doc_id := str_remove_all(doc_id, "\\.txt$")]
  
  # Remove rows with missing tokens
  parsed.post <- parsed.post[!is.na(token)]
  
  # Handle double tokens introduced by parsing
  parsed.post[, estDoubleMot := is.na(head_token_id) & is.na(upos)]
  dt.intrus <- parsed.post[estDoubleMot == TRUE, .(
    doc_id, term_id = c(term_id + 1, term_id + 2)
  )][, estIntrus := TRUE]
  
  parsed.post$compte <- TRUE
  parsed.post <- merge(parsed.post, dt.intrus, all = TRUE)
  parsed.post[estIntrus == TRUE, compte := FALSE]
  parsed.post[, c("estIntrus", "estDoubleMot") := NULL]
  
  # Remove punctuation and particles from counts
  parsed.post[upos %in% c("PUNCT", "PART"), compte := FALSE]
  
  # Correct copula to VERB
  parsed.post[dep_rel == "cop", upos := "VERB"]
  
  # Remove duplicates
  parsed.post <- parsed.post[!duplicated(parsed.post[, .(doc_id, term_id)])]
  
  # Clean columns
  cols_to_remove <- c("start", "end", "xpos", "deps")
  parsed.post <- parsed.post[, .SD, .SDcols = setdiff(names(parsed.post), cols_to_remove)]
  
  # Reclassify relative pronouns as PRON
  parsed.post[upos == "ADV" & feats == "PronType=Rel", upos := "PRON"]
  
  # Normalize tokens and lemmas
  parsed.post[upos != "PROPN", `:=`(
    token = str_to_lower(token),
    lemma = str_to_lower(lemma)
  )]
  parsed.post[, `:=`(
    token = str_replace_all(token, "œ", "oe"),
    lemma = str_replace_all(lemma, "œ", "oe")
  )]
  parsed.post[!is.na(token) & str_starts(token, "['-]"), 
              token := str_sub(token, 2)]
  
  # Sort for consistent ordering
  parsed.post <- parsed.post[order(doc_id, paragraph_id, sentence_id, term_id)]
  
  # Add lowercase token column
  parsed.post[, lower_token := tolower(token)]
  
  return(parsed.post)
}

