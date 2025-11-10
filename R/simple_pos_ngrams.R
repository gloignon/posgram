library(data.table)
library(tidytable)


# Core POS ngram summarizer
# This will generate n-grams of POS tags and summarize their counts and probabilities
summarize_pos_ngrams <- function(dt, n = 3, pos_col = "upos") {
  setorderv(dt, c("doc_id", "sentence_id", "token_id"))
  dt_tmp <- copy(dt)
  for (i in seq_len(n)) {
    dt_tmp[, paste0("pos_", i) := shift(get(pos_col), n - i, type = "lag"), by = .(doc_id, sentence_id)]
  }
  pos_cols <- paste0("pos_", 1:n)
  dt_tmp <- dt_tmp[complete.cases(dt_tmp[, ..pos_cols])]
  dt_tmp[, pos_ngram := do.call(paste, c(.SD, sep = "_")), .SDcols = pos_cols]
  dt_summary <- dt_tmp[, .N, by = pos_ngram]
  setnames(dt_summary, "N", "count")
  dt_summary[, n := n]
  dt_summary[, probability := count / sum(count)]
  setcolorder(dt_summary, c("n", "pos_ngram", "count", "probability"))
  return(dt_summary[])
}

# Wrapper function to generate POS n-grams from 1 to max_n
generate_pos_ngrams_from_1_to_n <- function(dt, max_n = 3, pos_col = "upos") {
  library(data.table)
  ngram_list <- lapply(1:max_n, function(n) summarize_pos_ngrams(dt, n = n, pos_col = pos_col))
  dt_all <- rbindlist(ngram_list, use.names = TRUE)
  setnames(dt_all, old = c("pos_ngram", "count", "n"), new = c("feature", "frequency", "ngram_size"), skip_absent = TRUE)
  return(dt_all[])
}

# Smoothed POS surprisal precomputation using backoff
precompute_backoff_pos_surprisal <- function(dt_ngrams, lambda = 0.4, max_n = 4) {
  library(data.table)
  
  log2_lambda <- -log2(lambda)
  dt <- copy(dt_ngrams)
  
  stopifnot(all(c("feature", "ngram_size", "frequency") %in% names(dt)))
  
  # Extract context and target
  dt[, context := fifelse(ngram_size > 1, gsub("_[^_]+$", "", feature), "")]
  dt[, target := sub(".*_", "", feature)]
  
  # Store surprisal tables with renamed columns
  surprisal_list <- list()
  for (n in 1:max_n) {
    dt_n <- dt[ngram_size == n]
    
    if (n == 1) {
      # Unigram: global frequency
      total <- sum(dt_n$frequency)
      dt_n[, paste0("s", n) := -log2(frequency / total)]
      dt_n[, key := target]
    } else {
      # Higher-order: conditional prob within context
      dt_n[, prob := frequency / sum(frequency), by = context]
      dt_n[, paste0("s", n) := -log2(pmax(prob, 1e-10))]
      dt_n[, key := paste(context, target, sep = "_")]
    }
    
    surprisal_list[[n]] <- dt_n[, .(key, surprisal = get(paste0("s", n)))]
    setnames(surprisal_list[[n]], "surprisal", paste0("s", n))
  }
  
  # Merge all n-gram surprisal tables by key
  dt_merged <- Reduce(function(x, y) merge(x, y, by = "key", all = TRUE), surprisal_list)
  
  # Compute backoff surprisal
  dt_merged[, pos_surprisal_backoff := NA_real_]
  for (n in max_n:1) {
    col_n <- paste0("s", n)
    dt_merged[is.na(pos_surprisal_backoff) & !is.na(get(col_n)),
              pos_surprisal_backoff := get(col_n) + (max_n - n) * log2_lambda]
  }
  
  dt_merged[, feature := key]
  
  # Split feature into context and target again
  dt_merged[, context := fifelse(grepl("_", feature), gsub("_[^_]+$", "", feature), "")]
  dt_merged[, target := sub(".*_", "", feature)]
  dt_merged[, ngram_size := pmax(1L, lengths(strsplit(feature, "_")))]  # number of tokens in feature
  
  # Return required structure
  return(dt_merged[, .(ngram_size, context, target, pos_surprisal_backoff)])
  
}

# Smoothed POS surprisal precomputation using Good-Turing
precompute_gt_pos_surprisal <- function(dt_ngrams, max_n = 3) {
  stopifnot(all(c("feature", "frequency") %in% names(dt_ngrams)))
  dt <- copy(dt_ngrams)
  
  # Ensure ngram_size
  if (!"ngram_size" %in% names(dt)) {
    if ("n" %in% names(dt)) setnames(dt, "n", "ngram_size") else stop("Missing 'ngram_size' column.")
  }
  
  # Split feature into context + target EXACTLY like annotate_pos_surprisal_gt()
  dt[, context := gsub("_[^_]+$", "", feature)]   # applies to all n; for unigrams = the tag itself
  dt[, target  := sub(".*_", "", feature)]
  
  # Integer counts for GT; drop zeros
  dt[, freq := as.integer(round(frequency))]
  dt <- dt[freq > 0L]
  
  # Count-of-counts by (ngram_size, freq) — same granularity as the annotator
  freq_counts <- dt[, .N, by = .(ngram_size, freq)]
  setnames(freq_counts, "N", "N_r")
  setorder(freq_counts, ngram_size, freq)
  
  # Good-Turing r* = (r+1) * N_{r+1} / N_r  (vectorized within ngram_size)
  freq_counts[, r_plus1   := shift(freq, type = "lead"), by = ngram_size]
  freq_counts[, N_r_plus1 := shift(N_r, type = "lead"),  by = ngram_size]
  freq_counts[, r_star    := (freq + 1) * (N_r_plus1 / N_r)]
  
  # Join r* back; DO NOT fallback (leave NA to force backoff just like the annotator)
  dt <- merge(
    dt,
    freq_counts[, .(ngram_size, freq, r_star)],
    by = c("ngram_size", "freq"),
    all.x = TRUE,
    sort = FALSE
  )
  
  # Normalize within (context, ngram_size) using ONLY r_star (NA propagates)
  dt[, context_prob_mass := sum(r_star, na.rm = TRUE), by = .(context, ngram_size)]
  dt[, prob_gt      := r_star / context_prob_mass]
  dt[, surprisal_gt := -log2(pmax(prob_gt, 1e-10))]  # keeps NA if r_star was NA
  
  # Output shape expected by annotate_with_precomputed_surprisal()
  out <- dt[, .(ngram_size, context, target, surprisal_gt)]
  setkey(out, ngram_size, context, target)
  unique(out)
}


# Annotator function using precomputed surprisal table
# (this is what you need to port to your project if you want to use precomputed POS surprisal)
annotate_with_precomputed_surprisal <- function(dt_corpus,
                                                   dt_surprisal,
                                                   surprisal_col = "surprisal",
                                                   max_n = 3) {
  stopifnot(all(c("doc_id","sentence_id","token_id","upos") %in% names(dt_corpus)))
  stopifnot(all(c("ngram_size","context","target", surprisal_col) %in% names(dt_surprisal)))
  
  dtc <- copy(dt_corpus)
  dts <- copy(dt_surprisal)
  setorder(dtc, doc_id, sentence_id, token_id)
  
  # 1) Lag columns up to (max_n - 1)
  if (max_n >= 2) {
    for (i in 1:(max_n - 1)) {
      dtc[, paste0("upos_m", i) := shift(upos, i, type = "lag"), by = .(doc_id)]
    }
  }
  
  # 2) Build context strings for each n: for n>=2 use lag concat; for n==1, we won’t use context
  if (max_n >= 2) {
    for (n in max_n:2) {
      lag_cols <- paste0("upos_m", (n - 1):1)
      dtc[, paste0("context_", n) := do.call(paste, c(.SD, sep = "_")), .SDcols = lag_cols]
    }
  }
  dtc[, target := upos]
  
  # 3) Merge each order separately; unigrams: join on target only (no context)
  used_cols <- character(0)
  for (n in max_n:1) {
    suffix <- paste0(n, "gram")
    col_out <- paste0("surprisal_", suffix)
    
    if (n == 1L) {
      dtn <- dts[ngram_size == 1L, .(target, surprisal = get(surprisal_col))]
      dtc <- merge(dtc, dtn, by = "target", all.x = TRUE, sort = FALSE)
    } else {
      ctx_col <- paste0("context_", n)
      dtn <- dts[ngram_size == n, .(context, target, surprisal = get(surprisal_col))]
      dtc <- merge(dtc, dtn, by.x = c(ctx_col, "target"), by.y = c("context", "target"),
                   all.x = TRUE, sort = FALSE)
    }
    setnames(dtc, "surprisal", col_out)
    used_cols <- c(used_cols, col_out)
  }
  
  # 4) Pick first non-NA from max_n ... 1
  # data.table::fcoalesce picks first non-NA left-to-right
  dtc[, pos_surprisal := fcoalesce(.SD), .SDcols = used_cols]
  
  # 5) Track which n was used (first non-NA)
  dtc[, pos_ngram_used := "unseen"]
  for (n in max_n:1) {
    col_n <- paste0("surprisal_", n, "gram")
    dtc[is.na(pos_surprisal) & !is.na(get(col_n)), pos_surprisal := get(col_n)]
    dtc[pos_ngram_used == "unseen" & !is.na(get(col_n)), pos_ngram_used := paste0(n, "gram")]
  }
  
  dtc[, .(doc_id, sentence_id, token_id, pos_surprisal, pos_ngram_used)]
}


# Main ----
set.seed(123)

dt_wiki_sent <- readRDS("corpora/wikiviki/parsed/wivico_wiki_sent_parsed_20240612.Rds")
dt_viki_sent <- readRDS("corpora/wikiviki/parsed/wivico_viki_sent_parsed_20240612.Rds")

# Exclude POS that are not relevant here 
# dt_wiki_sent <- dt_wiki_sent[!upos %in% c("PUNCT", "SYM", "X", "INTJ")]
# dt_viki_sent <- dt_viki_sent[!upos %in% c("PUNCT", "SYM", "X", "INTJ")]

# Wiki and Viki models ----
wiki_pos_grams <- generate_pos_ngrams_from_1_to_n(dt_wiki_sent, max_n = 3)
viki_pos_grams <- generate_pos_ngrams_from_1_to_n(dt_viki_sent, max_n = 3)

wiki_pos_gt <- precompute_gt_pos_surprisal(wiki_pos_grams, max_n = 3)
viki_pos_gt <- precompute_gt_pos_surprisal(viki_pos_grams, max_n = 3)

# Save
saveRDS(wiki_pos_gt, "pos_models/wiki_pos_gt_20240612.Rds")
saveRDS(viki_pos_gt, "pos_models/viki_pos_gt_20240612.Rds")

# now combine both 
dt_vikiwiki <- bind_rows(
  dt_wiki_sent,
  dt_viki_sent
)

wikiviki_pos_grams <- generate_pos_ngrams_from_1_to_n(dt_vikiwiki, max_n = 3)
wikiviki_pos_gt <- precompute_gt_pos_surprisal(wikiviki_pos_grams, max_n = 3)
saveRDS(wikiviki_pos_gt, "pos_models/wikiviki_pos_gt_20240612.Rds")

# test_surprisals <- annotate_with_precomputed_surprisal(
#   dt_corpus = dt_viki_sent,
#   dt_surprisal = wiki_pos_gt,
#   surprisal_col = "surprisal_gt",
#   max_n = 4
# )

# Experiment -----

# make a vector of unique sentences in dt_wiki_sent
v_unique_id <- dt_wiki_sent[, unique(paste0(doc_id, "_", sentence_id))]
# draw 80% for training

v_train_id <- sample(v_unique_id, size = 0.8 * length(v_unique_id))
v_test_id <- setdiff(v_unique_id, v_train_id)


# combine corpora
  # change doc_id to reflect source
dt_wiki_sent[, source := "wiki"]
dt_viki_sent[, source := "viki"]
dt_wivi_sent <- bind_rows(dt_wiki_sent, dt_viki_sent)


# split into train/test, 80% train, 20% test
# make unique sentence identifiers
dt_wivi_sent[, sentence_uid := paste0(doc_id, "_", sentence_id)]
dt_train <- dt_wivi_sent[paste0(doc_id, "_", sentence_id) %in% v_train_id]
dt_test <- dt_wivi_sent[paste0(doc_id, "_", sentence_id) %in% v_test_id]
# change doc_id to reflect source
dt_train[, doc_id := paste0(source, "_", doc_id)]
dt_test[, doc_id := paste0(source, "_", doc_id)]
table(dt_train$source)
table(dt_test$source)

## POS models ----
pos_ngrams <- generate_pos_ngrams_from_1_to_n(dt_train, max_n = 3)

# Method #1
pos_model <- precompute_gt_pos_surprisal(pos_ngrams, max_n = 3)
dt_surprised <- annotate_with_precomputed_surprisal(
  dt_corpus = dt_test,
  dt_surprisal = pos_model,
  surprisal_col = "surprisal_gt",
  max_n = 3
)
table(dt_surprised$pos_ngram_used)
  # merge back with my original data
dt_surprised <- merge(
  dt_surprised,
  dt_test[, .(doc_id, sentence_id, token_id, source, sentence_uid)],
  by = c("doc_id", "sentence_id", "token_id"),
  all.x = TRUE
)

# Method #2
dt_surprised_2 <- annotate_pos_surprisal_gt(
  dt_corpus = dt_test,
  dt_pos_ngrams = pos_ngrams
)
table(dt_surprised$pos_ngram_used)  # Method 2


# mean surprisal by sentence, then overall mean
dt_surprised %>%
  group_by(sentence_uid, source) %>%
  summarize(mean_pos_surprisal = mean(pos_surprisal, na.rm = TRUE)) %>%
  rstatix::cohens_d(mean_pos_surprisal ~ source)

dt_surprised_2 %>%
  group_by(sentence_uid, source) %>%
  summarize(mean_pos_surprisal = mean(pos_surprisal, na.rm = TRUE)) %>%
  rstatix::cohens_d(mean_pos_surprisal ~ source)

# 
# # plot raw surprisals (boxplot) by source
# library(ggplot2)
# ggplot(dt_surprise_stats, aes(x = source, y = mean_pos_surprisal)) +
#   geom_boxplot(notch = TRUE, outliers = FALSE) +
#   theme_minimal() +
#   labs(title = "Sentence POS Surprisal by Source",
#        x = "Source",
#        y = "POS Surprisal (bits)") +
#   theme(legend.position = "none")
# 
# library(glmmTMB)
# # mixed logistic
# mlr1 <- glmmTMB(
#   mean_pos_surprisal ~ source + (1 | sentence_uid),
#   data = dt_surprise_stats
# )
# car::Anova(mlr1)
# # marginal plot
# ggeffects::ggpredict(mlr1, terms = "source")
# sjPlot::plot_model(mlr1, type = "pred", terms = "source")
# 
# # now we can summarize the differences like champs
# dt_surprise_stats %>%
#   select(-sentence_uid) %>%
#   pivot_wider(names_from = source, values_from = c(mean_pos_surprisal, sd_pos_surprisal)) %>%
#   # calculate differences
#   mutate(diff_mean_pos_surprisal = mean_pos_surprisal_wiki - mean_pos_surprisal_viki) %>%
#   summarize(overall_mean_diff_pos_surprisal = mean(diff_mean_pos_surprisal, na.rm = TRUE),
#           overall_sd_diff_pos_surprisal = sd(diff_mean_pos_surprisal, na.rm = TRUE),
#           n_sentences = n())

# group_by(source) %>%
  # summarize(overall_mean_pos_surprisal = mean(mean_pos_surprisal, na.rm = TRUE),
  #           overall_sd_pos_surprisal = sd(mean_pos_surprisal, na.rm = TRUE),
  #           n_sentences = n())

