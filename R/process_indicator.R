#' Process a single indicator based on YAML configuration
#'
#' This is the main function that processes any indicator using its
#' configuration file, making the system scalable to 300+ indicators.
#'
#' @param indicator_id ID of the indicator to process
#' @param config_path Path to the YAML configuration file
#' @param output_dir Directory to save processed data
#' @param force_download Whether to force re-download of source data
#' @return List with processing results and metadata
#' @export
process_indicator <- function(indicator_id,
                             config_path = "/workspace/packages/data-r/config/indicators.yml",  # Use absolute path
                             output_dir = "/workspace/outputs",  # Use absolute path
                             force_download = FALSE) {

  # Load configuration
  config <- yaml::read_yaml(config_path)

  if (!indicator_id %in% names(config)) {
    stop(glue::glue("Indicator '{indicator_id}' not found in configuration"))
  }

  indicator_config <- config[[indicator_id]]

  message(glue::glue("Processing indicator: {indicator_config$names$es}"))

  # Step 1: Find and download Excel file
  excel_url <- find_inei_excel_url(
    page_url = indicator_config$source$page_url,
    target_title = indicator_config$source$target_title,
    keywords = indicator_config$source$keywords
  )

  if (is.na(excel_url)) {
    stop(glue::glue("Could not find Excel file for indicator {indicator_id}"))
  }

  message(glue::glue("Found Excel URL: {excel_url}"))

  # Step 2: Download Excel file
  temp_file <- tempfile(fileext = ".xlsx")
  success <- download_with_retry(excel_url, temp_file)

  if (!success) {
    stop(glue::glue("Failed to download Excel file for {indicator_id}"))
  }

  # Step 3: Process Excel file based on configuration
  processed_data <- process_excel_file(temp_file, indicator_config)

  # Step 4: Save outputs in requested formats
  output_files <- save_indicator_outputs(
    data = processed_data,
    indicator_id = indicator_id,
    config = indicator_config,
    output_dir = output_dir
  )

  # Cleanup
  file.remove(temp_file)

  return(list(
    indicator_id = indicator_id,
    config = indicator_config,
    data = processed_data,
    output_files = output_files,
    processed_at = Sys.time()
  ))
}

#' Process Excel file according to indicator configuration
#'
#' @param excel_file Path to Excel file
#' @param config Indicator configuration list
#' @return Processed data frame
process_excel_file <- function(excel_file, config) {

  # Read Excel file
  df_raw <- readxl::read_xlsx(excel_file, sheet = config$processing$sheet_number)

  # Handle multiple header rows if specified
  if ("header_rows" %in% names(config$processing)) {
    df_raw <- process_multiple_headers(df_raw, config$processing$header_rows)
  }

  # Skip header rows and start from data
  data_start <- config$processing$data_start_row
  df <- df_raw |>
    dplyr::slice(-seq_len(data_start - 1)) |>
    clean_indicator_names()

  # Handle department marker (like "Departamento" row)
  if ("department_marker" %in% names(config$processing)) {
    marker <- config$processing$department_marker
    text_col <- names(df)[1]

    row_dept <- which(normalize_text(df[[text_col]]) == normalize_text(marker))
    if (length(row_dept) > 0) {
      df <- df |> dplyr::slice((row_dept + 1):dplyr::n())
    }
  }

  # Set geographic column name
  df <- df |> dplyr::rename(Departamento = 1)

  # Filter out unwanted rows
  if ("exclude_rows" %in% names(config$processing)) {
    exclude_values <- config$processing$exclude_rows
    df <- df |>
      dplyr::filter(!Departamento %in% exclude_values)
  }

  if ("exclude_patterns" %in% names(config$processing)) {
    exclude_patterns <- paste(config$processing$exclude_patterns, collapse = "|")
    df <- df |>
      dplyr::filter(!stringr::str_detect(normalize_text(Departamento), exclude_patterns))
  }

  # CRITICAL FIX: Convert all value columns to character before pivoting
  # This prevents type mismatch errors when some columns are numeric and others are character
  df <- df |>
    dplyr::mutate(dplyr::across(-Departamento, as.character))

  # Convert to long format if specified
  if (config$output$structure == "long") {
    df <- convert_to_long_format(df, config)
  }

  # Convert data types
  df <- apply_column_types(df, config$output$columns)

  return(df)
}

#' Convert wide format data to long format
#'
#' @param df Data frame in wide format
#' @param config Indicator configuration
#' @return Data frame in long format
convert_to_long_format <- function(df, config) {

  # Identify value columns (typically year columns or year_gender combinations)
  value_cols <- names(df)[-1]  # Exclude first column (Departamento)

  # Check if this is a gender-disaggregated indicator
  if ("Sexo" %in% purrr::map_chr(config$output$columns, "name")) {
    # Handle gender-disaggregated data (like pension coverage)
    df_long <- df |>
      tidyr::pivot_longer(
        cols = -Departamento,
        names_to = c("Año", "Sexo"),
        names_sep = "_",
        values_to = "Porcentaje"
      ) |>
      dplyr::mutate(Año = as.integer(Año))
  } else {
    # Handle simple time series data (like malnutrition)
    df_long <- df |>
      tidyr::pivot_longer(
        cols = -Departamento,
        names_to = "Año",
        values_to = "Tasa"
      ) |>
      dplyr::mutate(Año = as.integer(stringr::str_extract(Año, "\\d{4}")))
  }

  return(df_long)
}

#' Apply column type conversions
#'
#' @param df Data frame
#' @param column_specs Column specifications from config
#' @return Data frame with converted types
apply_column_types <- function(df, column_specs) {
  for (col_spec in column_specs) {
    col_name <- col_spec$name
    col_type <- col_spec$type

    if (col_name %in% names(df)) {
      df[[col_name]] <- switch(col_type,
        "integer" = as.integer(df[[col_name]]),
        "numeric" = parse_indicator_numbers(df[[col_name]]),
        "character" = as.character(df[[col_name]]),
        df[[col_name]]
      )
    }
  }
  return(df)
}

#' Save indicator outputs in multiple formats
#'
#' @param data Processed data frame
#' @param indicator_id Indicator identifier
#' @param config Indicator configuration
#' @param output_dir Output directory
#' @return Vector of output file paths
save_indicator_outputs <- function(data, indicator_id, config, output_dir = "/workspace/outputs") {

  output_files <- character(0)

  for (format in config$output$format) {
    filename <- switch(format,
      "csv" = glue::glue("{indicator_id}.csv"),
      "parquet" = glue::glue("{indicator_id}.parquet"),
      "arrow" = glue::glue("{indicator_id}.arrow")
    )

    filepath <- file.path(output_dir, format, filename)
    fs::dir_create(dirname(filepath))

    switch(format,
      "csv" = readr::write_csv(data, filepath),
      "parquet" = arrow::write_parquet(data, filepath),
      "arrow" = arrow::write_feather(data, filepath)
    )

    output_files <- c(output_files, filepath)
    message(glue::glue("Saved: {filepath}"))
  }

  return(output_files)
}