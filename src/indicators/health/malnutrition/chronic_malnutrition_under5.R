# ==============================
# DSS Indicator: Chronic Malnutrition Under 5 Years
# ==============================

library(here)
source(here("packages/data-r/R/utils_normalization.R"))
source(here("packages/data-r/R/utils_web_scraping.R"))
source(here("packages/data-r/R/process_indicator.R"))

# Method 1: Use the generic processor (RECOMMENDED)
# This is the new way - configuration-driven processing
process_chronic_malnutrition <- function(output_dir = here("outputs")) {
  result <- process_indicator(
    indicator_id = "chronic_malnutrition_under5",
    output_dir = output_dir
  )

  message("✅ Chronic malnutrition indicator processed successfully")
  message(glue::glue("📊 Processed {nrow(result$data)} rows"))
  message(glue::glue("💾 Output files: {paste(result$output_files, collapse = ', ')}"))

  return(result)
}

# Method 2: Direct processing (for custom logic if needed)
# This shows how you could still customize the processing while using shared utilities
process_chronic_malnutrition_custom <- function(output_dir = here("outputs")) {

  # Parameters (now these could come from config)
  page_url <- "https://m.inei.gob.pe/estadisticas/indice-tematico/sociales/"
  target_title <- "TASA DE DESNUTRICIÓN CRÓNICA DE NIÑOS/AS MENORES DE 5 AÑOS, SEGÚN DEPARTAMENTO, 2013 - 2023"
  keywords <- c("desnutricion", "cronica", "5", "departament")

  message("🔍 Searching for chronic malnutrition data...")

  # Step 1: Find Excel URL using shared utility
  excel_url <- find_inei_excel_url(page_url, target_title, keywords)

  if (is.na(excel_url)) {
    stop("❌ Could not locate the Excel file URL")
  }

  message(glue::glue("📁 Found Excel file: {excel_url}"))

  # Step 2: Download file using shared utility
  temp_file <- tempfile(fileext = ".xlsx")
  success <- download_with_retry(excel_url, temp_file)

  if (!success) {
    stop("❌ Failed to download Excel file")
  }

  # Step 3: Process Excel file
  message("📋 Processing Excel data...")

  df_raw <- readxl::read_xlsx(temp_file, sheet = 1)

  # Filter rows with all NA values (empty rows)
  df_raw <- df_raw |>
    dplyr::filter(dplyr::if_all(2:ncol(df_raw), ~ !is.na(.)))

  # Use shared utilities for data cleaning
  df <- df_raw |>
    janitor::row_to_names(row_number = 1) |>  # First row as headers
    clean_indicator_names() |>                # Shared cleaning function
    dplyr::rename(Departamento = 1) |>        # Standardize geographic column
    # Filter out unwanted rows
    dplyr::filter(
      !is.na(Departamento),
      !Departamento %in% c("Urbana", "Rural"),
      !stringr::str_detect(normalize_text(Departamento),
                          "total|nota|fuente|ministerio|establecida")
    )

  # Convert year columns to numeric using shared utility
  df <- df |>
    dplyr::rename_with(~ stringr::str_replace(., "^(x|X)(?=\\d{4}$)", ""), -Departamento) |>
    dplyr::mutate(dplyr::across(-Departamento, parse_indicator_numbers))

  # Convert to long format (tidy data)
  df_long <- df |>
    tidyr::pivot_longer(
      cols = -Departamento,
      names_to = "Año",
      values_to = "Tasa"
    ) |>
    dplyr::mutate(
      Año = as.integer(stringr::str_extract(Año, "\\d{4}")),
      Tasa = as.numeric(Tasa)
    ) |>
    dplyr::filter(!is.na(Tasa))  # Remove missing values

  # Step 4: Save outputs
  message("💾 Saving output files...")

  # Ensure output directories exist
  fs::dir_create(file.path(output_dir, "csv"))
  fs::dir_create(file.path(output_dir, "parquet"))

  # Save as CSV (human-readable)
  csv_file <- file.path(output_dir, "csv", "chronic_malnutrition_under5.csv")
  readr::write_csv(df_long, csv_file)

  # Save as Parquet (efficient for dashboards)
  parquet_file <- file.path(output_dir, "parquet", "chronic_malnutrition_under5.parquet")
  arrow::write_parquet(df_long, parquet_file)

  # Cleanup
  file.remove(temp_file)

  message("✅ Processing completed successfully!")
  message(glue::glue("📊 Processed {nrow(df_long)} department-year observations"))
  message(glue::glue("📅 Years covered: {min(df_long$Año)} - {max(df_long$Año)}"))
  message(glue::glue("🌍 Departments: {length(unique(df_long$Departamento))}"))
  message(glue::glue("💾 CSV: {csv_file}"))
  message(glue::glue("💾 Parquet: {parquet_file}"))

  return(list(
    data = df_long,
    files = c(csv_file, parquet_file),
    metadata = list(
      indicator = "chronic_malnutrition_under5",
      source_url = excel_url,
      processed_at = Sys.time(),
      n_departments = length(unique(df_long$Departamento)),
      year_range = range(df_long$Año),
      n_observations = nrow(df_long)
    )
  ))
}

# Main execution function
if (!interactive()) {
  # Run when called from command line or Turborepo
  result <- process_chronic_malnutrition()
  cat("✅ Chronic malnutrition indicator processing completed\n")
}