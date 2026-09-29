# ============================================================
# Indicador DSS
# ============================================================
# Fuente: World Bank
# Indicador: Gasto público en educación, total (% del PIB)
# Codigo indicador: SE.XPD.TOTL.GD.ZS
# Nivel geografico: País
# Frecuencia: Anual
# Region: America
# ============================================================

library(here)
library(jsonlite)
library(dplyr)
library(arrow)
library(readr)
library(fs)
library(glue)
library(countrycode) # country.name.es country.name.en country.name.pt iso3c continent

source(here("herramientas/descargas/banco-mundial.R"))

process_gasto_educ_pib <- function(output_dir = here("outputs")) {
  codigo_indicador <- "SE.XPD.TOTL.GD.ZS"

  educ_raw <- descargar_banco_mundial(
    indicator_code = codigo_indicador
  )

  educ_PIB <- educ_raw |>
    transmute(
      iso3 = countryiso3code,
      anio = as.integer(date),
      valor = as.numeric(value)
    )

  codigos_paises <- codelist |>
    select(
      country.name.es,
      country.name.en,
      iso3c,
      continent,
      region
    ) |>
    filter(continent == "Americas")

  # Dashboard contract:
  # iso3, territorio, cod_local, anio, valor.
  # At regional level each country is a territory; cod_local does not apply.
  educ_PIB <- educ_PIB |>
    inner_join(codigos_paises, by = c("iso3" = "iso3c")) |>
    transmute(
      iso3,
      territorio = country.name.es,
      cod_local = NA_character_,
      anio,
      valor
    ) |>
    filter(!is.na(anio), !is.na(valor)) |>
    arrange(
      territorio,
      anio
    )

  # Save dashboard-ready outputs following the src/indicators pattern.
  dir_create(file.path(output_dir, "csv"))
  dir_create(file.path(output_dir, "parquet"))

  csv_file <- file.path(output_dir, "csv", "gasto-educ-pib.csv")
  parquet_file <- file.path(output_dir, "parquet", "gasto-educ-pib.parquet")

  write_csv(educ_PIB, csv_file)
  write_parquet(educ_PIB, parquet_file)

  message(glue("✅ Education expenditure data processed and saved to: {output_dir}"))
  message(glue("💾 CSV: {csv_file}"))
  message(glue("💾 Parquet: {parquet_file}"))

  return(list(
    data = educ_PIB,
    output_files = c(csv_file, parquet_file)
  ))
}

#if (!interactive()) {
  result <- process_gasto_educ_pib()
#  cat("✅ Education expenditure processing completed.\n")
#}

