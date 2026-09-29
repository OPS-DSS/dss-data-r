# ============================================================
# Regional analytics: prioritized health indicators x DSS
# ============================================================
# Statistical computation belongs to the R data pipeline.
# Countries are the unit of analysis and indicators are joined by iso3 + anio.
# ============================================================

library(here)
library(dplyr)
library(tidyr)
library(readr)
library(arrow)
library(fs)
library(purrr)
library(sf)
library(RColorBrewer)
library(rnaturalearth)

MIN_COUNTRIES <- 10L

spearman_summary <- function(x, y) {
  ok <- is.finite(x) & is.finite(y)
  x <- x[ok]
  y <- y[ok]
  n <- length(x)
  if (n < MIN_COUNTRIES || length(unique(x)) < 2L || length(unique(y)) < 2L) {
    return(NULL)
  }
  test <- suppressWarnings(cor.test(x, y, method = "spearman", exact = FALSE))
  rho <- unname(test$estimate)

  # Fisher-z approximation retained for compatibility with PAM-DSS correlation chart.
  rho_clamped <- max(-0.9999, min(0.9999, rho))
  z <- atanh(rho_clamped)
  se <- 1 / sqrt(n - 3)

  tibble(
    correlacion = rho,
    ci_lower = tanh(z - 1.96 * se),
    ci_upper = tanh(z + 1.96 * se),
    p_value = test$p.value,
    n = as.integer(n)
  )
}

read_regional_indicator <- function(path, slug) {
  data <- read_parquet(path)

  # Advanced Analytics always uses the total population.
  # Stratified rows remain available in the source parquet for DSS charts.
  if ("sexo" %in% names(data)) {
    data <- data |>
      filter(sexo == "Total")
  }

  data |>
    transmute(
      iso3 = as.character(iso3),
      territorio = as.character(territorio),
      anio = as.integer(anio),
      valor = as.numeric(valor),
      slug = slug
    ) |>
    filter(!is.na(iso3), !is.na(anio), is.finite(valor)) |>
    distinct(iso3, anio, .keep_all = TRUE)
}

process_regional_analytics <- function(
    output_dir = here("outputs"),
    priority_slugs = c(
      "razon-mortalidad-materna",
      "tuberculosis"
    ),
    dss_slugs = c(
      "gasto-educ-pib",
      "uso-internet",
      "gini"
    )) {

  parquet_dir <- file.path(output_dir, "parquet")
  csv_dir <- file.path(output_dir, "csv")
  dir_create(parquet_dir)
  dir_create(csv_dir)

  required <- c(priority_slugs, dss_slugs)
  missing <- required[!file.exists(file.path(parquet_dir, paste0(required, ".parquet")))]
  if (length(missing) > 0L) {
    stop("Missing regional indicator parquet(s): ", paste(missing, collapse = ", "))
  }

  indicators <- set_names(
    map(required, ~ read_regional_indicator(
      file.path(parquet_dir, paste0(.x, ".parquet")), .x
    )),
    required
  )

  scatter <- crossing(priorizado = priority_slugs, dss = dss_slugs) |>
    mutate(data = map2(priorizado, dss, function(p, d) {
      health <- indicators[[p]] |>
        select(iso3, territorio, anio, valor_salud = valor)
      determinant <- indicators[[d]] |>
        select(iso3, anio, valor_dss = valor)
      inner_join(health, determinant, by = c("iso3", "anio")) |>
        mutate(priorizado = p, dss = d, .before = 1)
    })) |>
    select(data) |>
    unnest(data) |>
    arrange(priorizado, dss, anio, territorio)

  correlations <- scatter |>
    group_by(priorizado, dss, anio) |>
    group_split() |>
    map_dfr(function(df) {
      result <- spearman_summary(df$valor_dss, df$valor_salud)
      if (is.null(result)) return(tibble())
      bind_cols(
        df |> slice(1) |> select(priorizado, dss, anio),
        result
      )
    }) |>
    group_by(priorizado, anio) |>
    arrange(desc(abs(correlacion)), .by_group = TRUE) |>
    mutate(rank_abs = row_number(), top10 = rank_abs <= 10L) |>
    ungroup()

  trends <- bind_rows(
    map_dfr(priority_slugs, function(slug) {
      indicators[[slug]] |>
        transmute(tipo = "priorizado", indicador = slug, iso3, territorio, anio, valor)
    }),
    map_dfr(dss_slugs, function(slug) {
      indicators[[slug]] |>
        transmute(tipo = "dss", indicador = slug, iso3, territorio, anio, valor)
    })
  ) |>
    arrange(tipo, indicador, anio, territorio)

  files <- c(
    correlations_csv = file.path(csv_dir, "regional-correlations.csv"),
    correlations_parquet = file.path(parquet_dir, "regional-correlations.parquet"),
    scatter_csv = file.path(csv_dir, "regional-scatter.csv"),
    scatter_parquet = file.path(parquet_dir, "regional-scatter.parquet"),
    trends_csv = file.path(csv_dir, "regional-trends.csv"),
    trends_parquet = file.path(parquet_dir, "regional-trends.parquet")
  )

  write_csv(correlations, files[["correlations_csv"]])
  write_parquet(correlations, files[["correlations_parquet"]])
  write_csv(scatter, files[["scatter_csv"]])
  write_parquet(scatter, files[["scatter_parquet"]])
  write_csv(trends, files[["trends_csv"]])
  write_parquet(trends, files[["trends_parquet"]])


  # ── Regional GeoJSONs for PAM-DSS analytical maps ──────────────────────────
  geojson_dir <- file.path(output_dir, "geojson")
  dir_create(geojson_dir)

  # Same spatial workflow used by the SMV analytics: sf geometry + st_write().
  # Natural Earth supplies the base country polygons; ISO3 is the stable join key.
  americas_sf <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf") |>
    filter(continent %in% c("North America", "South America")) |>
    transmute(
      iso3 = as.character(iso_a3),
      territorio = as.character(name_long),
      geometry
    ) |>
    filter(iso3 != "-99") |>
    sf::st_make_valid()

  ylord <- RColorBrewer::brewer.pal(5, "YlOrRd")
  bivariate_colors <- matrix(c(
    "#e8e8e8", "#ace4e4", "#5ac8c8",
    "#dfb0d6", "#a5b8c5", "#5a9ab5",
    "#be64ac", "#8c62aa", "#3b4994"
  ), nrow = 3, byrow = TRUE)

  classify_tercile <- function(x) {
    out <- rep(NA_integer_, length(x))
    ok <- is.finite(x)
    if (sum(ok) < 2L || length(unique(x[ok])) < 2L) return(out)
    r <- rank(x[ok], ties.method = "average")
    out[ok] <- pmin(2L, floor((r - 1) * 3 / length(r)))
    out
  }

  sequential_colors <- function(x) {
    out <- rep("#CCCCCC", length(x))
    ok <- is.finite(x)
    if (!any(ok)) return(out)
    if (length(unique(x[ok])) < 2L) {
      out[ok] <- ylord[3L]
      return(out)
    }
    cls <- pmin(5L, pmax(1L, ceiling(rank(x[ok], ties.method = "average") * 5 / sum(ok))))
    out[ok] <- ylord[cls]
    out
  }

  write_single_map <- function(slug, rows) {
    for (yr in sort(unique(rows$anio))) {
      values <- rows |>
        filter(anio == yr) |>
        distinct(iso3, .keep_all = TRUE) |>
        select(iso3, value = valor)
      out <- americas_sf |>
        left_join(values, by = "iso3") |>
        mutate(color = sequential_colors(value)) |>
        select(iso3, territorio, value, color, geometry)
      sf::st_write(
        out,
        file.path(geojson_dir, paste0(slug, "-", yr, ".geojson")),
        driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE
      )
    }
  }

  walk(priority_slugs, ~ write_single_map(.x, indicators[[.x]]))
  walk(dss_slugs, ~ write_single_map(.x, indicators[[.x]]))

  # Bivariate ODS-health x DSS maps. Classification is calculated in R.
  crossing(priorizado = priority_slugs, dss = dss_slugs) |>
    pwalk(function(priorizado, dss) {
      pair_rows <- scatter |>
        filter(.data$priorizado == priorizado, .data$dss == dss)
      for (yr in sort(unique(pair_rows$anio))) {
        values <- pair_rows |>
          filter(anio == yr) |>
          distinct(iso3, .keep_all = TRUE) |>
          mutate(
            health_class = classify_tercile(valor_salud),
            dss_class = classify_tercile(valor_dss),
            color = map2_chr(health_class, dss_class, function(hc, dc) {
              if (is.na(hc) || is.na(dc)) "#CCCCCC"
              else bivariate_colors[hc + 1L, dc + 1L]
            })
          ) |>
          select(
            iso3,
            value = valor_dss,
            health_value = valor_salud,
            health_class,
            dss_class,
            color
          )

        out <- americas_sf |>
          left_join(values, by = "iso3") |>
          mutate(color = coalesce(color, "#CCCCCC")) |>
          select(
            iso3, territorio, value, health_value,
            health_class, dss_class, color, geometry
          )

        sf::st_write(
          out,
          file.path(
            geojson_dir,
            paste0("bivariate-", priorizado, "-", dss, "-", yr, ".geojson")
          ),
          driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE
        )
      }
    })

  message("Regional GeoJSON maps saved.")

  message("Regional analytics saved: correlations, scatter and trends.")
  invisible(list(
    correlations = correlations,
    scatter = scatter,
    trends = trends,
    output_files = unname(files)
  ))
}

#if (!interactive()) {
  process_regional_analytics()
#}
