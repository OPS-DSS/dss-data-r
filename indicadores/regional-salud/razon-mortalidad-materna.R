# ============================================================
# Indicador DSS
# ============================================================
# Fuente: Naciones Unidas - Global SDG Database
# Indicador: Razón de mortalidad materna
# Codigo indicador: SH_STA_MORT
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

process_razon_mortalidad_materna <- function(output_dir = here("outputs")) {
  codigo_indicador <- "SH_STA_MORT"

  mortalidad_materna_raw <- descargar_un_ods(
    indicator_code = codigo_indicador
  )

  mortalidad_materna <- mortalidad_materna_raw |>
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
  mortalidad_materna <- mortalidad_materna |>
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

  csv_file <- file.path(output_dir, "csv", "razon-mortalidad-materna.csv")
  parquet_file <- file.path(output_dir, "parquet", "razon-mortalidad-materna.parquet")

  write_csv(mortalidad_materna, csv_file)
  write_parquet(mortalidad_materna, parquet_file)

  message(glue("✅ Maternal mortality data processed and saved to: {output_dir}"))
  message(glue("💾 CSV: {csv_file}"))
  message(glue("💾 Parquet: {parquet_file}"))

  return(list(
    data = mortalidad_materna,
    output_files = c(csv_file, parquet_file)
  ))
}

#if (!interactive()) {
  result <- process_razon_mortalidad_materna()
 # cat("✅ Maternal mortality processing completed.\n")
#}
