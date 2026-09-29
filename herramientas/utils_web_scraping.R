#' Find Excel file URL from INEI indicator page
#'
#' Generic function to find Excel files on INEI pages using title matching
#' and keyword fallback strategies.
#'
#' @param page_url URL of the INEI indicator page
#' @param target_title Expected title of the indicator
#' @param keywords Vector of keywords for fallback search
#' @param max_retries Maximum number of retry attempts
#' @return Character string with Excel file URL or NA if not found
#' @export
find_inei_excel_url <- function(page_url, target_title, keywords = NULL, max_retries = 3) {

  target_norm <- normalize_text(target_title)

  for (attempt in 1:max_retries) {
    tryCatch({
      pg <- rvest::read_html(page_url)

      links_df <- pg |>
        rvest::html_elements("a") |>
        (\(x) tibble::tibble(
          text = rvest::html_text2(x),
          href = rvest::html_attr(x, "href")
        ))() |>
        dplyr::mutate(
          text_norm = normalize_text(text),
          href_abs = xml2::url_absolute(href, page_url)
        )

      # Strategy 1: Exact title match
      hit <- links_df |>
        dplyr::filter(text_norm == target_norm)

      # Strategy 2: Keyword-based search if exact match fails
      if (nrow(hit) == 0 && !is.null(keywords)) {
        conditions <- purrr::map(keywords, ~ rlang::expr(stringr::str_detect(text_norm, !!.x)))
        combined_condition <- purrr::reduce(conditions, ~ rlang::expr(!!.x & !!.y))

        hit <- links_df |>
          dplyr::filter(!!combined_condition)
      }

      if (nrow(hit) == 0) {
        return(NA_character_)
      }

      href_candidate <- hit$href_abs[[1]]

      # If already Excel file, return it
      if (stringr::str_detect(href_candidate, "\\.xlsx?$")) {
        return(href_candidate)
      }

      # If intermediate page, search within it
      return(find_excel_in_page(href_candidate))

    }, error = function(e) {
      if (attempt == max_retries) {
        warning(glue::glue("Failed to scrape after {max_retries} attempts: {e$message}"))
        return(NA_character_)
      }
      Sys.sleep(2^attempt)  # Exponential backoff
    })
  }
}

#' Search for Excel files within a specific page
#'
#' @param page_url URL to search for Excel files
#' @return Excel file URL or NA
find_excel_in_page <- function(page_url) {
  tryCatch({
    pg2 <- rvest::read_html(page_url)

    links2 <- pg2 |>
      rvest::html_elements("a") |>
      (\(x) tibble::tibble(
        text = rvest::html_text2(x),
        href = rvest::html_attr(x, "href")
      ))() |>
      dplyr::mutate(
        text_norm = normalize_text(text),
        href_abs = xml2::url_absolute(href, page_url)
      )

    xlsx_candidates <- links2 |>
      dplyr::filter(stringr::str_detect(href_abs, "\\.xlsx?$"))

    if (nrow(xlsx_candidates) > 0) {
      # Score candidates by relevance
      xlsx_candidates <- xlsx_candidates |>
        dplyr::mutate(
          score = (
            stringr::str_detect(text_norm, "excel") +
            stringr::str_detect(text_norm, "descargar") +
            stringr::str_detect(text_norm, "xlsx")
          )
        ) |>
        dplyr::arrange(dplyr::desc(score))

      return(xlsx_candidates$href_abs[[1]])
    }

    return(NA_character_)

  }, error = function(e) {
    warning(glue::glue("Error searching Excel in page {page_url}: {e$message}"))
    return(NA_character_)
  })
}

#' Download file with retry logic
#'
#' @param url URL to download
#' @param destfile Destination file path
#' @param max_retries Maximum retry attempts
#' @return TRUE if successful, FALSE otherwise
download_with_retry <- function(url, destfile, max_retries = 3) {
  for (attempt in 1:max_retries) {
    tryCatch({
      download.file(url, destfile, mode = "wb", quiet = TRUE)
      return(TRUE)
    }, error = function(e) {
      if (attempt == max_retries) {
        warning(glue::glue("Download failed after {max_retries} attempts: {e$message}"))
        return(FALSE)
      }
      Sys.sleep(2^attempt)
    })
  }
}