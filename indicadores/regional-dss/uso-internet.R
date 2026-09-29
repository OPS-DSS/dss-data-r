# ============================================================
# Indicador DSS
# ============================================================
# Fuente: World Bank
# Indicador: Individuos que utilizan Internet (% de la población)
# Codigo indicador total: IT.NET.USER.ZS
# Nivel geografico: País
# Estrato: Sexo
# Categorias: Total / Mujeres / Hombres
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


process_internet <- function(output_dir = here("outputs")) {
  
  # ----------------------------------------------------------
  # Códigos de indicadores World Bank por sexo
  # ----------------------------------------------------------
  
  indicadores <- tibble::tribble(
    ~codigo_indicador,    ~sexo,
    "IT.NET.USER.ZS",     "Total",
    "IT.NET.USER.FE.ZS",  "Mujeres",
    "IT.NET.USER.MA.ZS",  "Hombres"
  )
  
  
  # ----------------------------------------------------------
  # Descargar y combinar indicadores
  # ----------------------------------------------------------
  
  internet <- lapply(seq_len(nrow(indicadores)), function(i) {
    
    codigo <- indicadores$codigo_indicador[i]
    estrato <- indicadores$sexo[i]
    
    message(
      glue("🌐 Descargando Internet - {estrato} ({codigo})...")
    )
    
    internet_raw <- descargar_banco_mundial(
      indicator_code = codigo
    )
    
    internet_raw |>
      transmute(
        iso3 = countryiso3code,
        anio = as.integer(date),
        valor = as.numeric(value),
        sexo = estrato
      )
    
  }) |>
    bind_rows()
  
  
  # ----------------------------------------------------------
  # Códigos de países de América
  # ----------------------------------------------------------
  
  codigos_paises <- codelist |>
    select(
      country.name.es,
      country.name.en,
      iso3c,
      continent,
      region
    ) |>
    filter(continent == "Americas")
  
  
  # ----------------------------------------------------------
  # Dashboard contract:
  # iso3, territorio, cod_local, anio, valor, sexo
  # ----------------------------------------------------------
  
  internet <- internet |>
    inner_join(
      codigos_paises,
      by = c("iso3" = "iso3c")
    ) |>
    transmute(
      iso3,
      territorio = country.name.es,
      cod_local = NA_character_,
      anio,
      valor,
      sexo
    ) |>
    filter(
      !is.na(anio),
      !is.na(valor)
    ) |>
    arrange(
      territorio,
      sexo,
      anio
    )
  
  
  # ----------------------------------------------------------
  # Guardar archivos para dashboard
  # ----------------------------------------------------------
  
  dir_create(file.path(output_dir, "csv"))
  dir_create(file.path(output_dir, "parquet"))
  
  csv_file <- file.path(
    output_dir,
    "csv",
    "uso-internet.csv"
  )
  
  parquet_file <- file.path(
    output_dir,
    "parquet",
    "uso-internet.parquet"
  )
  
  write_csv(internet, csv_file)
  write_parquet(internet, parquet_file)
  
  
  # ----------------------------------------------------------
  # Mensajes
  # ----------------------------------------------------------
  
  message(
    glue("✅ Uso de Internet procesado y guardado en: {output_dir}")
  )
  
  message(glue("💾 CSV: {csv_file}"))
  message(glue("💾 Parquet: {parquet_file}"))
  
  
  return(
    list(
      data = internet,
      output_files = c(
        csv_file,
        parquet_file
      )
    )
  )
}


# ------------------------------------------------------------
# Ejecutar
# ------------------------------------------------------------

#if (!interactive()) {
result <- process_internet()
#  cat("✅ Internet usage processing completed.\n")
#}