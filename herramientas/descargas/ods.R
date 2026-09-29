# ============================================================
# Indicador ODS
# Fuente: Naciones Unidas - Global SDG Database
# ============================================================

library(here)
library(jsonlite)
library(dplyr)
library(arrow)
library(readr)
library(fs)
library(glue)
library(httr)


descargar_un_ods <- function(indicator_code) {
  
  url <- "https://unstats.un.org/SDGAPI/v1/sdg/Series/Data"
  
  pagina <- 1
  lista_batches <- list()
  i <- 1
  
  repeat {
    
    message(
      glue("  Downloading UN SDG indicator {indicator_code} - page {pagina}...")
    )
    
    response <- tryCatch(
      GET(
        url,
        query = list(
          seriesCode = indicator_code,
          page = pagina,
          pageSize = 1000
        )
      ),
      error = function(e) {
        stop(
          glue(
            "Error downloading UN SDG indicator {indicator_code}: ",
            "{conditionMessage(e)}"
          ),
          call. = FALSE
        )
      }
    )
    
    stop_for_status(response)
    
    resultado <- content(
      response,
      as = "text",
      encoding = "UTF-8"
    ) |>
      fromJSON(flatten = TRUE)
    
    datos <- resultado$data
    
    if (is.null(datos) || nrow(datos) == 0) break
    
    lista_batches[[i]] <- datos
    
    if (!is.null(resultado$totalPages) &&
        pagina >= resultado$totalPages) break
    
    pagina <- pagina + 1
    i <- i + 1
  }
  
  datos_final <- bind_rows(lista_batches)
  
  message(
    glue("  Records downloaded: {nrow(datos_final)}")
  )
  
  return(datos_final)
}
