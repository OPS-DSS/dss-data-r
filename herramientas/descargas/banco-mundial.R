# ============================================================
# Indicador Banco Mundial
# Fuente: World Bank
# ============================================================

library(here)
library(jsonlite)
library(dplyr)
library(arrow)
library(readr)
library(fs)
library(glue)



descargar_banco_mundial <- function(indicator_code) {
  
  url <- glue(
    "https://api.worldbank.org/v2/country/all/indicator/{indicator_code}",
    "?format=json&per_page=20000"
  )
  
  message(
    glue("  Downloading World Bank indicator {indicator_code}...")
  )
  
  resultado <- tryCatch(
    fromJSON(url, flatten = TRUE),
    error = function(e) {
      stop(
        glue(
          "Error downloading World Bank indicator {indicator_code}: ",
          "{conditionMessage(e)}"
        ),
        call. = FALSE
      )
    }
  )
  
  if (length(resultado) < 2 || is.null(resultado[[2]])) {
    stop(
      "World Bank API returned no observations.",
      call. = FALSE
    )
  }
  
  datos <- resultado[[2]]
  
  message(
    glue("  Records downloaded: {nrow(datos)}")
  )
  
  return(datos)
}

