# ==============================
# DSS Indicator: Suicide Mortality - Huila
# ==============================
# Source: Observatorio de Salud del Huila
# Indicator: Mortalidad por suicidio (por 100.000 hab.)
# Stratifier: sexo (gender)
# ==============================

library(here)
library(readxl)
library(dplyr)
library(arrow)
library(readr)
library(fs)
library(glue)

process_suicide_huila <- function(output_dir = here("outputs")) {
  url <- "https://www.huila.gov.co/observatoriosalud/loader.php?lServicio=Tools2&lTipo=descargas&lFuncion=descargar&idFile=84079"

  temp_file <- tempfile(fileext = ".xlsx")

  message("⬇️ Downloading suicide mortality data from Huila observatory...")
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
  suicidio_raw <- read_excel(temp_file)

  suicidio <- suicidio_raw |>
    mutate(
      iso3      = "COL",
      valor     = Suicidios / Población * 100000,
      indicador = "Mortalidad por suicidio (100.000 hab.)"
    ) |>
    rename(
      cod_subnacional = Regional,
      cod_local       = `Cod - Terr`,
      anio            = Año,
      sexo            = Sexo,
      territorio      = Territorio
    ) |>
    select(iso3, territorio, cod_subnacional, cod_local, anio, sexo, valor, indicador) |>
    filter(!is.na(valor), !is.na(anio))

  # Create output directories
  dir_create(file.path(output_dir, "csv"))
  dir_create(file.path(output_dir, "parquet"))

  # Save outputs
  csv_file     <- file.path(output_dir, "csv",     "suicide_huila.csv")
  parquet_file <- file.path(output_dir, "parquet", "suicide_huila.parquet")

  write_csv(suicidio, csv_file)
  write_parquet(suicidio, parquet_file)

  file.remove(temp_file)

  message(glue("✅ Processed {nrow(suicidio)} rows"))
  message(glue("📅 Years: {min(suicidio$anio)} - {max(suicidio$anio)}"))
  message(glue("👤 Sex categories: {paste(unique(suicidio$sexo), collapse = ', ')}"))
  message(glue("💾 CSV:     {csv_file}"))
  message(glue("💾 Parquet: {parquet_file}"))

  return(list(
    data         = suicidio,
    output_files = c(csv_file, parquet_file)
  ))
}

# Main execution — called from Turborepo or command line
if (!interactive()) {
  result <- process_suicide_huila()
  cat("✅ Suicide mortality (Huila) processing completed\n")
}
