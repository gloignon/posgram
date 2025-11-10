library(jsonlite)
library(data.table)
library(udpipe)

read_jsonl_dump <- function(path) {
  lines <- readLines(path, warn = FALSE)
  parsed <- lapply(lines, fromJSON, simplifyVector = TRUE)
  
  dt <- rbindlist(lapply(parsed, function(x) {
    data.table(
      index = x$index,
      hard_title = x$hard$title,
      easy_title = x$easy$title,
      hard_text = paste(x$hard$sentences, collapse = " "),
      easy_text = paste(x$easy$sentences, collapse = " ")
    )
  }))
  
  return(dt)
}

udmodel_french <-
  udpipe_load_model(file = "ud_models/french_gsd-remix_2.udpipe") #chargement du modèle linguistique

# Read the json file
# from https://zenodo.org/records/11371932
dt_dump <- read_jsonl_dump("corpora/wikiviki/single_file/vikidia-fr_sentences.jsonl")

# make doc names
dt_dump[, doc_id_hard := paste0("wiki_", index)]
dt_dump[, doc_id_easy := paste0("viki_", index)]

dt_wiki <- udpipe(
  x = data.frame(doc_id = dt_dump$doc_id_hard, text = dt_dump$hard_text),
  object = udmodel_french,
  parallel.cores = 15,
  trace = TRUE
)

dt_viki <- udpipe(
  x = data.frame(doc_id = dt_dump$doc_id_easy, text = dt_dump$easy_text),
  object = udmodel_french,
  parallel.cores = 15,
  trace = TRUE
)

# Save parsed data ----
setDT(dt_wiki)
setDT(dt_viki)
saveRDS(dt_wiki, "corpora/wikiviki/parsed/wiki_parsed_20240612.Rds")
saveRDS(dt_viki, "corpora/wikiviki/parsed/viki_parsed_20240612.Rds")
saveRDS(dt_dump, "corpora/wikiviki/parsed/wikiviki_dump.Rds")
