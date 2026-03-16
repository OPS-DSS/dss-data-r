# ==============================
# DSS Indicator: Huila Municipal Map - Education Indicators
# ==============================
# Source: GADM (boundaries) + datos.gov.co (education data)
# Description: Downloads municipality boundaries for the Huila department
#   (Colombia), joins education indicator data for the most recent year,
#   computes a 5-class YlOrRd colour palette per indicator, and exports
#   one GeoJSON per indicator (for the Leaflet choropleth map) and a
#   combined CSV (tabular download).
#
# GeoJSON properties per feature:
#   - NAME_2 : municipality name (from GADM)
#   - value  : indicator value for the most recent year (NA if no data)
#   - color  : hex colour from YlOrRd palette (#CCCCCC if no data)
# ==============================

library(here)
library(geodata)
library(terra)
library(RColorBrewer)
library(sf)
library(dplyr)
library(readr)
library(glue)
library(jsonlite)
library(stringi)

#' Download full Socrata dataset with pagination
descargar_socrata_completa <- function(base_url, batch_size = 50000, extra_query = NULL) {
  offset <- 0
  lista_batches <- list()
  i <- 1

  repeat {
    url <- paste0(
      base_url,
      "?$limit=", batch_size,
      "&$offset=", offset,
      if (!is.null(extra_query)) paste0("&", extra_query) else ""
    )
    message(glue("  Descargando desde offset = {offset}"))
    batch <- tryCatch(
      fromJSON(url, flatten = TRUE),
      error = function(e) stop(
        glue("Error al descargar datos desde '{url}': {conditionMessage(e)}"),
        call. = FALSE
      )
    )
    if (nrow(batch) == 0) break
    lista_batches[[i]] <- batch
    if (nrow(batch) < batch_size) break
    offset <- offset + batch_size
    i <- i + 1
  }

  bind_rows(lista_batches)
}

#' Normalise a character vector for fuzzy municipality name matching.
#' Converts to lowercase, strips diacritics, and trims whitespace.
normalize_name <- function(x) {
  x |>
    tolower() |>
    stri_trans_general("Latin-ASCII") |>
    trimws()
}

#' Compute a 5-class YlOrRd colour for a numeric vector.
#' Municipalities with NA values receive "#CCCCCC" (grey).
compute_color <- function(values) {
  palette <- RColorBrewer::brewer.pal(5, "YlOrRd")

  # If all values are NA, return all grey and avoid quantile/cut errors
  if (length(values) == 0L || all(is.na(values))) {
    return(rep("#CCCCCC", length(values)))
  }

  # Compute quantile breaks (may contain duplicates if variance is low)
  breaks <- quantile(values, probs = seq(0, 1, length.out = 6), na.rm = TRUE)
  breaks <- unique(breaks)

  # If we do not have at least two distinct break points, fall back
  if (length(breaks) < 2L) {
    rng <- range(values, na.rm = TRUE)

    # All non-NA values are identical: assign a single colour to non-NA values
    if (is.finite(rng[1]) && rng[1] == rng[2]) {
      colors <- rep(palette[3L], length(values))
      colors[is.na(values)] <- "#CCCCCC"
      return(colors)
    }

    # Otherwise, use equal-interval breaks over the observed range
    breaks <- seq(rng[1], rng[2], length.out = 6)
  }

  classes <- cut(values, breaks = breaks, include.lowest = TRUE, labels = FALSE)
  colors  <- palette[classes]
  colors[is.na(colors)] <- "#CCCCCC"
  colors
}

process_huila_map <- function(output_dir = here("outputs")) {

  # ── Municipality boundaries ──────────────────────────────────────────────────
  message("⬇️  Downloading Colombia municipality boundaries from GADM (level 2)...")
  colombia_muni <- geodata::gadm(country = "COL", level = 2, path = tempdir())

  message("📋 Filtering to Huila department...")
  huila_muni <- colombia_muni[colombia_muni$NAME_1 == "Huila", ]
  n_muni     <- nrow(huila_muni)
  message(glue("   Found {n_muni} municipalities in Huila"))

  # ── Education data for all Huila municipalities ──────────────────────────────
  message("⬇️  Downloading education data for Huila from datos.gov.co...")
  base_url <- "https://www.datos.gov.co/resource/nudc-7mev.json"

  select_fields <- paste(
    c(
      "a_o", "municipio", "departamento",
      "cobertura_bruta", "cobertura_neta",
      "deserci_n", "aprobaci_n", "reprobaci_n", "repitencia"
    ),
    collapse = ","
  )
  where_clause <- URLencode("departamento = 'Huila'", reserved = TRUE)
  extra_query  <- glue("$select={select_fields}&$where={where_clause}")

  edu_raw <- descargar_socrata_completa(base_url, extra_query = extra_query)

  message("📋 Processing education data...")
  edu <- edu_raw |>
    mutate(
      anio            = as.integer(a_o),
      cobertura_bruta = as.numeric(cobertura_bruta),
      cobertura_neta  = as.numeric(cobertura_neta),
      deserci_n       = as.numeric(deserci_n),
      aprobaci_n      = as.numeric(aprobaci_n),
      reprobaci_n     = as.numeric(reprobaci_n),
      repitencia      = as.numeric(repitencia)
    ) |>
    filter(!is.na(anio))

  if (nrow(edu) == 0) {
    stop(
      "No education records were returned for Huila after processing. ",
      "This may indicate an API issue, a schema change, or a mismatch in the query filter."
    )
  }

  # Use the most recent year with available data
  latest_year  <- max(edu$anio, na.rm = TRUE)
  message(glue("   Using year: {latest_year}"))
  edu_latest <- edu |>
    filter(anio == latest_year) |>
    mutate(nombre_norm = normalize_name(municipio))

  # ── Join boundaries ↔ education ──────────────────────────────────────────────
  huila_base_df <- as.data.frame(huila_muni) |>
    mutate(nombre_norm = normalize_name(NAME_2))

  joined <- huila_base_df |>
    left_join(
      edu_latest |>
        select(
          nombre_norm, municipio,
          cobertura_bruta, cobertura_neta,
          deserci_n, aprobaci_n, reprobaci_n, repitencia
        ),
      by = "nombre_norm"
    )

  n_matched <- sum(!is.na(joined$cobertura_bruta))
  message(glue("   Matched {n_matched}/{n_muni} municipalities with education data"))

  # ── Export ───────────────────────────────────────────────────────────────────
  dir.create(file.path(output_dir, "geojson"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(output_dir, "csv"),     recursive = TRUE, showWarnings = FALSE)

  huila_sf_base <- sf::st_as_sf(huila_muni)

  # One GeoJSON per education indicator
  indicator_map <- list(
    cobertura_bruta = "huila_cobertura_bruta",
    cobertura_neta  = "huila_cobertura_neta",
    deserci_n       = "huila_desercion",
    aprobaci_n      = "huila_aprobacion",
    reprobaci_n     = "huila_reprobacion",
    repitencia      = "huila_repitencia"
  )

  output_files <- character(0)

  for (ind_col in names(indicator_map)) {
    file_stem <- indicator_map[[ind_col]]
    values    <- joined[[ind_col]]
    colors    <- compute_color(values)

    huila_sf_ind <- huila_sf_base |>
      mutate(
        value = values,
        color = colors
      ) |>
      select(NAME_2, value, color)

    geojson_file <- file.path(output_dir, "geojson", paste0(file_stem, ".geojson"))
    sf::st_write(huila_sf_ind, geojson_file, delete_dsn = TRUE)
    output_files <- c(output_files, geojson_file)
    message(glue("💾 GeoJSON ({ind_col}): {geojson_file}"))
  }

  # Combined CSV with all indicators
  csv_df <- joined |>
    select(
      NAME_2, municipio,
      cobertura_bruta, cobertura_neta,
      deserci_n, aprobaci_n, reprobaci_n, repitencia
    )
  csv_file <- file.path(output_dir, "csv", "huila_map.csv")
  write_csv(csv_df, csv_file)
  output_files <- c(output_files, csv_file)
  message(glue("💾 CSV: {csv_file}"))

  message(glue("✅ Processed {n_muni} municipalities, matched {n_matched} with education data (year {latest_year})"))

  return(list(
    data         = csv_df,
    output_files = output_files
  ))
}

# Main execution — called from Turborepo or command line
if (!interactive()) {
  result <- process_huila_map()
  cat("✅ Huila map processing completed\n")
}
