# ============================================================
# Indicador de contexto
# ============================================================
# Fuente: World Bank
# Indicador: Esperanza de vida al nacer, total (años)
# Código: SP.DYN.LE00.IN
# ============================================================

library(here)
library(dplyr)
library(arrow)
library(readr)
library(fs)
library(countrycode)

source(here("herramientas/descargas/banco-mundial.R"))

process_esperanza_vida <- function(output_dir = here("outputs")) {
  
  datos_raw <- descargar_banco_mundial(
    indicator_code = "SP.DYN.LE00.IN"
  )
  
  paises <- codelist |>
    select(country.name.es, iso3c, continent) |>
    filter(continent == "Americas")
  
  datos <- datos_raw |>
    transmute(
      iso3 = countryiso3code,
      anio = as.integer(date),
      valor = as.numeric(value)
    ) |>
    inner_join(paises, by = c("iso3" = "iso3c")) |>
    transmute(
      iso3,
      territorio = country.name.es,
      cod_local = NA_character_,
      anio,
      valor
    ) |>
    filter(!is.na(valor)) |>
    arrange(territorio, anio)
  
  dir_create(file.path(output_dir, "csv"))
  dir_create(file.path(output_dir, "parquet"))
  
  write_csv(
    datos,
    file.path(output_dir, "csv", "esperanza-vida.csv")
  )
  
  write_parquet(
    datos,
    file.path(output_dir, "parquet", "esperanza-vida.parquet")
  )
  
  return(datos)
}

esperanza_vida <- process_esperanza_vida()
