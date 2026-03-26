# ==============================
# DSS Indicator: Maternal Mortality Rate
# ==============================
# Source: Observatorio de Salud del Huila
# Indicator: Mortalidad materna (por 100.000 hab.)
# ==============================

library(here)
library(readxl)
library(dplyr)
library(arrow)
library(readr)
library(fs)
library(glue)
source(here("packages/data-r/R/util_gaps.R"))

process_maternal_mortality_rate <- function(output_dir = here("outputs")) {
  url <- "https://www.huila.gov.co/observatoriosalud/loader.php?lServicio=Tools2&lTipo=descargas&lFuncion=descargar&idFile=85285"

  temp_file <- tempfile(fileext = ".xlsx")

  message("⬇️ Downloading maternal mortality data from Huila observatory...")

  tryCatch(
    download.file(
      url = url, destfile = temp_file,
      mode = "wb", quiet = TRUE
    ),
    error = function(e) {
      msg <- conditionMessage(e)
      stop(glue("❌ Failed to download: {msg}"))
    }
  )

  message("📋 Processing data...")
  mortalidad_materna_raw <- read_excel(temp_file)

  mortalidad_materna <- mortalidad_materna_raw |>
    mutate(
      iso3 = "COL",
      valor = `Defunciones maternas` / Nacimientos * 100000,
      indicador = "Mortalidad materna (100.000 nacidos vivos)"
    ) |>
    rename(
      cod_local = `Cod_Terr`,
      anio = Año
    ) |>
    select(
      iso3,
      Territorio,
      cod_local,
      anio,
      valor
    )

  # Create output directories
  dir_create(file.path(output_dir, "csv"))
  dir_create(file.path(output_dir, "parquet"))

  # Save outputs
  maternal_mortality_rate_csv_file <- file.path(output_dir, "csv", "maternal_mortality_rate.csv")
  maternal_mortality_rate_parquet_file <- file.path(output_dir, "parquet", "maternal_mortality_rate.parquet")

  write_csv(mortalidad_materna, maternal_mortality_rate_csv_file)
  write_parquet(mortalidad_materna, maternal_mortality_rate_parquet_file)

  file.remove(temp_file)

  message(glue("✅ Maternal mortality rate data processed and saved to: {output_dir}"))
  message(glue("💾 CSV: {maternal_mortality_rate_csv_file}"))
  message(glue("💾 Parquet: {maternal_mortality_rate_parquet_file}"))

  return(list(
    data = mortalidad_materna,
    output_files = c(maternal_mortality_rate_csv_file, maternal_mortality_rate_parquet_file)
  ))
}

# Main execution - called from Turborepo or command line
if (!interactive()) {
  result <- process_maternal_mortality_rate()
  cat("✅ Maternal mortality rate data processing completed.\n")
}
