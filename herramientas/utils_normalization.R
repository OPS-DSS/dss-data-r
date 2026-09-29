#' Normalize text for consistent comparison
#'
#' Normalizes text by converting to lowercase, removing accents/ñ,
#' and compacting spaces. This is the common normalization function
#' used across all indicators.
#'
#' @param x Character vector to normalize
#' @return Character vector with normalized text
#' @export
#' @examples
#' normalize_text("TASA DE DESNUTRICIÓN CRÓNICA")
#' # Returns: "tasa de desnutricion cronica"
normalize_text <- function(x) {
  x |>
    stringi::stri_trans_general("Latin-ASCII") |>
    stringr::str_to_lower() |>
    stringr::str_squish()
}

#' Clean column names using janitor standards
#'
#' Standardizes column names across all indicators using janitor::clean_names
#' and additional custom cleaning rules.
#'
#' @param df Data frame to clean
#' @return Data frame with cleaned column names
#' @export
clean_indicator_names <- function(df) {
  df |>
    janitor::clean_names() |>
    # Remove leading X from year columns (common in Excel imports)
    dplyr::rename_with(~ stringr::str_replace(.x, "^(x|X)(?=\\d{4})", ""),
                       dplyr::matches("^(x|X)\\d{4}"))
}

#' Parse numeric values robustly
#'
#' Safely converts character values to numeric, handling common
#' formatting issues in Latin American statistical data.
#'
#' @param x Character vector to convert
#' @return Numeric vector
#' @export
parse_indicator_numbers <- function(x) {
  readr::parse_number(as.character(x))
}