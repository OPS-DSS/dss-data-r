# =========================================================
# San Martín del Valle - Simulación de acceso geográfico
# Proporción de embarazadas que viven a más de una hora
# del centro de salud más cercano
# =========================================================

# ---------------------------
# 1. Carpeta de salida
# ---------------------------
library(here)

output_dir <- here("outputs")

if (!dir.exists(output_dir)) {
  stop("La carpeta de salida no existe. Revisa la ruta.")
}

# ---------------------------
# 2. Paquetes
# ---------------------------
paquetes <- c("dplyr", "tidyr", "readr", "writexl", "ggplot2")

instalados <- rownames(installed.packages())
for (p in paquetes) {
  if (!(p %in% instalados)) install.packages(p)
}

library(dplyr)
library(tidyr)
library(readr)
library(writexl)
library(ggplot2)

# ---------------------------
# 3. Semilla
# ---------------------------
set.seed(4321)

# ---------------------------
# 4. Leer base territorial
# ---------------------------
sim_csv_file <- file.path(output_dir, "csv", "SMV_map.csv")

if (!file.exists(sim_csv_file)) {
  stop("No existe 'SMV_map.csv'. Primero debes guardar la base territorial.")
}

smv_base <- read_csv(sim_csv_file, show_col_types = FALSE) %>%
  mutate(
    tipo_zona = case_when(
      tipo_zona == "Urbano central" ~ "urbano",
      tipo_zona == "Periurbano" ~ "periurbano",
      tipo_zona == "Rural" ~ "rural",
      TRUE ~ tolower(tipo_zona)
    )
  )

if (!all(c("NAME_2", "tipo_zona") %in% names(smv_base))) {
  stop("El archivo SMV_map.csv debe contener las columnas 'NAME_2' y 'tipo_zona'.")
}

# ---------------------------
# 5. Parámetros generales
# ---------------------------
anios <- 2016:2025
grupos_edad <- c("10-14", "15-19", "20-34", "35-49")

# 3 rurales + 1 periurbano
barrios_criticos <- c(
  "La Cañada",
  "El Mirador",
  "Los Pinos",
  "Nueva Esperanza"
)

# ---------------------------
# 6. Base barrio-año-edad
# ---------------------------
base <- expand_grid(
  anio = anios,
  NAME_2 = smv_base$NAME_2,
  grupo_edad = grupos_edad
) %>%
  left_join(smv_base, by = "NAME_2")

# ---------------------------
# 7. Tamaño de población embarazada simulada
# ---------------------------
base <- base %>%
  mutate(
    n_embarazadas = case_when(
      tipo_zona == "urbano" & grupo_edad == "10-14" ~ sample(2:6, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "10-14" ~ sample(2:7, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "10-14" ~ sample(1:6, n(), replace = TRUE),
      tipo_zona == "urbano" & grupo_edad == "15-19" ~ sample(8:20, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "15-19" ~ sample(8:18, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "15-19" ~ sample(5:15, n(), replace = TRUE),
      tipo_zona == "urbano" & grupo_edad == "20-34" ~ sample(35:90, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "20-34" ~ sample(22:65, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "20-34" ~ sample(12:45, n(), replace = TRUE),
      tipo_zona == "urbano" & grupo_edad == "35-49" ~ sample(10:30, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "35-49" ~ sample(8:22, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "35-49" ~ sample(5:18, n(), replace = TRUE),
      TRUE ~ sample(5:15, n(), replace = TRUE)
    )
  )

# ---------------------------
# 8. Efectos del modelo
# ---------------------------
# Gradiente esperado:
# urbano < periurbano < rural

efecto_zona <- c(
  "urbano" = -0.18,
  "periurbano" = 0.12,
  "rural" = 0.55
)

efecto_edad <- c(
  "10-14" = 0.08,
  "15-19" = 0.04,
  "20-34" = 0.00,
  "35-49" = 0.03
)

# Variabilidad intra-zona por barrio
efecto_barrio <- smv_base %>%
  mutate(
    efecto_barrio = rnorm(
      n(),
      mean = case_when(
        tipo_zona == "urbano" ~ -0.04,
        tipo_zona == "periurbano" ~ 0.02,
        tipo_zona == "rural" ~ 0.06,
        TRUE ~ 0
      ),
      sd = 0.08
    ),
    efecto_barrio = ifelse(
      NAME_2 %in% barrios_criticos,
      efecto_barrio + 0.18,
      efecto_barrio
    )
  ) %>%
  select(NAME_2, efecto_barrio)

base <- base %>%
  left_join(efecto_barrio, by = "NAME_2") %>%
  mutate(
    t = anio - min(anios),
    tendencia_anual = -0.025 * t
  )

# ---------------------------
# 9. Función logística
# ---------------------------
inv_logit <- function(x) 1 / (1 + exp(-x))

# ---------------------------
# 10. Simular proporción > 1 hora
# ---------------------------
base <- base %>%
  mutate(
    logit_p = -0.75 +
      efecto_zona[tipo_zona] +
      efecto_edad[grupo_edad] +
      efecto_barrio +
      tendencia_anual +
      rnorm(n(), 0, 0.12),
    prob_mas_1h = inv_logit(logit_p),
    prob_mas_1h = pmin(pmax(prob_mas_1h, 0.02), 0.95),
    n_mas_1h = rbinom(n(), size = n_embarazadas, prob = prob_mas_1h),
    valor = n_mas_1h / n_embarazadas
  )

# ---------------------------
# 11. Salidas finales
# ---------------------------

# 11.1 Total municipio
total_municipio <- base %>%
  group_by(anio) %>%
  summarise(
    valor = sum(n_mas_1h, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    sexo = "Mujeres",
    grupo_edad = "Todas las edades",
    zona = "Total"
  ) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 11.2 Municipio por edad
municipio_edad <- base %>%
  group_by(anio, grupo_edad) %>%
  summarise(
    valor = sum(n_mas_1h, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    sexo = "Mujeres",
    zona = "Total"
  ) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 11.3 Municipio por zona
municipio_zona <- base %>%
  group_by(anio, tipo_zona) %>%
  summarise(
    valor = sum(n_mas_1h, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    sexo = "Mujeres",
    grupo_edad = "Todas las edades"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 11.4 Municipio por zona y edad
municipio_zona_edad <- base %>%
  group_by(anio, tipo_zona, grupo_edad) %>%
  summarise(
    valor = sum(n_mas_1h, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    sexo = "Mujeres"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 11.5 Barrio total
barrio_total <- base %>%
  group_by(anio, NAME_2, tipo_zona) %>%
  summarise(
    valor = sum(n_mas_1h, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    sexo = "Mujeres",
    grupo_edad = "Todas las edades"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 11.6 Barrio por edad
barrio_edad <- base %>%
  group_by(anio, NAME_2, tipo_zona, grupo_edad) %>%
  summarise(
    valor = sum(n_mas_1h, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    sexo = "Mujeres"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# Unir todo
proporcion_embarazadas_mas_1h_final <- bind_rows(
  total_municipio,
  municipio_edad,
  municipio_zona,
  municipio_zona_edad,
  barrio_total,
  barrio_edad
) %>%
  arrange(NAME_2, anio, zona, grupo_edad)

# ---------------------------
# 12. Guardar archivos
# ---------------------------

csv_dir <- file.path(output_dir, "csv")
parquet_dir <- file.path(output_dir, "parquet")

if (!dir.exists(csv_dir)) dir.create(csv_dir, recursive = TRUE)
if (!dir.exists(parquet_dir)) dir.create(parquet_dir, recursive = TRUE)

archivo_csv <- file.path(csv_dir, "journey_time_municipal.csv")
write_csv(proporcion_embarazadas_mas_1h_final, archivo_csv)

archivo_parquet <- file.path(parquet_dir, "journey_time_municipal.parquet")
write_parquet(proporcion_embarazadas_mas_1h_final, archivo_parquet)
