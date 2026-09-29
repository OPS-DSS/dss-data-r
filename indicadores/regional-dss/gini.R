# ============================================================
# Indicador DSS
# ============================================================
# Fuente: World Bank
# Indicador: Coeficiente/Índice de Gini
# Codigo indicador: SI.POV.GINI
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

process_gini <- function(output_dir = here("outputs")) {
  codigo_indicador <- "SI.POV.GINI"

  gini_raw <- descargar_banco_mundial(
    indicator_code = codigo_indicador
  )

  gini <- gini_raw |>
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
  gini <- gini |>
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

  csv_file <- file.path(output_dir, "csv", "gini.csv")
  parquet_file <- file.path(output_dir, "parquet", "gini.parquet")

  write_csv(gini, csv_file)
  write_parquet(gini, parquet_file)

  message(glue("Indice o Coeficiente de Gini data processed and saved to: {output_dir}"))
  message(glue("💾 CSV: {csv_file}"))
  message(glue("💾 Parquet: {parquet_file}"))

  return(list(
    data = gini,
    output_files = c(csv_file, parquet_file)
  ))
}

#if (!interactive()) {
  result <- process_gini()
#  cat("Indice de Gini processing completed.\n")
#}

