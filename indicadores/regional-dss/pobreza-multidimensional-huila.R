# Indicador DSS: Incidencia de pobreza multidimensional, Huila (2018-2025)
# Fuente: DANE, anexo departamental de Pobreza Multidimensional 2025.
# Adaptado del archivo "Pobreza multidimensional.R" proporcionado por el equipo.
# Ejecutar desde la raiz del repositorio. Requiere readxl, dplyr, tidyr, readr, arrow.
#
# Salida: outputs/csv/pobreza-multidimensional-huila.csv
#         outputs/parquet/pobreza-multidimensional-huila.parquet
# Columnas: iso3, Territorio, cod_local, anio, sexo, zona, etnia, valor
# valor en porcentaje (escala 0-100), NO proporcion.

library(readxl)
library(dplyr)
library(tidyr)
library(readr)
library(arrow)

process_pobreza_multidimensional_huila <- function(output_dir = "outputs") {
  url <- "https://www.dane.gov.co/files/operaciones/PM/anex-PMultidimensional-Departamental-2025.xlsx"
  archivo <- tempfile(fileext = ".xlsx")
  on.exit(unlink(archivo), add = TRUE)
  download.file(url, destfile = archivo, mode = "wb", quiet = TRUE)

  ipm <- read_excel(archivo, sheet = "IPM_Departamentos",
                   range = "A14:Y46", col_names = FALSE)
  names(ipm) <- c(
    "Departamento",
    as.vector(t(outer(2018:2025, c("Total", "Cabecera", "Rural"), paste, sep = "_")))
  )

  parse_valor <- function(x) {
    if (is.numeric(x)) return(as.numeric(x))
    readr::parse_number(as.character(x),
                        locale = readr::locale(decimal_mark = ",", grouping_mark = "."))
  }

  resultado <- ipm %>%
    filter(trimws(as.character(Departamento)) == "Huila") %>%
    pivot_longer(cols = -Departamento, names_to = c("anio", "zona"),
                 names_sep = "_", values_to = "valor") %>%
    mutate(
      anio = as.integer(anio),
      zona = recode(zona, Total = "Total", Cabecera = "Urbano", Rural = "Rural"),
      valor = parse_valor(valor),
      iso3 = "COL", Territorio = "Huila", cod_local = "41",
      sexo = "Total", etnia = "Total"
    ) %>%
    select(iso3, Territorio, cod_local, anio, sexo, zona, etnia, valor) %>%
    arrange(anio, zona)

  if (nrow(resultado) != 24L || anyNA(resultado$valor)) {
    stop("Validacion fallida: se esperan 24 valores no faltantes (8 anos x 3 zonas).")
  }
  if (any(resultado$valor < 0 | resultado$valor > 100)) {
    stop("Valores fuera del rango esperado 0-100; revisar escala de DANE.")
  }

  dir.create(file.path(output_dir, "csv"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(output_dir, "parquet"), recursive = TRUE, showWarnings = FALSE)
  write_csv(resultado, file.path(output_dir, "csv", "pobreza-multidimensional-huila.csv"))
  write_parquet(resultado, file.path(output_dir, "parquet", "pobreza-multidimensional-huila.parquet"))
  invisible(resultado)
}

if (!interactive()) {
  process_pobreza_multidimensional_huila()
}
