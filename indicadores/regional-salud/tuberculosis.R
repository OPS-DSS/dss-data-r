# ============================================================
# Indicador DSS
# ============================================================
# Fuente: Naciones Unidas - Global SDG Database
# Indicador: Incidencia de Tuberculosis (por 100.000 hab.)
# Codigo indicador: SH_TBS_INCD
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
library(httr)
library(countrycode) # country.name.es country.name.en country.name.pt iso3c continent

source(here("herramientas/descargas/ods.R"))

process_tuberculosis <- function(output_dir = here("outputs")) {
  codigo_indicador <- "SH_TBS_INCD"

  tuberculosis_raw <- descargar_un_ods(
    indicator_code = codigo_indicador
  )

  tuberculosis <- tuberculosis_raw |>
    transmute(
      country = geoAreaName,
      anio = as.integer(timePeriodStart),
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
  tuberculosis <- tuberculosis |>
    inner_join(codigos_paises, by = c("country" = "country.name.en")) |>
    transmute(
      iso3 = iso3c,
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

  csv_file <- file.path(output_dir, "csv", "tuberculosis.csv")
  parquet_file <- file.path(output_dir, "parquet", "tuberculosis.parquet")

  write_csv(tuberculosis, csv_file)
  write_parquet(tuberculosis, parquet_file)

  message(glue("Incidencia de Tuberculosis data processed and saved to: {output_dir}"))
  message(glue("💾 CSV: {csv_file}"))
  message(glue("💾 Parquet: {parquet_file}"))

  return(list(
    data = tuberculosis,
    output_files = c(csv_file, parquet_file)
  ))
}

#if (!interactive()) {
  result <- process_tuberculosis()
 # cat("Tuberculosis processing completed.\n")
#}
