# =========================================================
# San Martín del Valle - Simulación de cobertura de transporte
# Cobertura del transporte público subsidiado hacia centros
# de salud desde barrios periféricos
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
paquetes <- c("dplyr", "tidyr", "readr", "writexl", "ggplot2", "sf", "scales")

instalados <- rownames(installed.packages())
for (p in paquetes) {
  if (!(p %in% instalados)) install.packages(p)
}

library(dplyr)
library(tidyr)
library(readr)
library(writexl)
library(ggplot2)
library(sf)
library(scales)

# ---------------------------
# 3. Semilla
# ---------------------------
set.seed(7891)

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

# Barrios con mejor priorización del subsidio de transporte
barrios_priorizados <- c(
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
  left_join(smv_base, by = "NAME_2") %>%
  filter(tipo_zona %in% c("periurbano", "rural"))

# ---------------------------
# 7. Población embarazada simulada
# ---------------------------
base <- base %>%
  mutate(
    n_embarazadas = case_when(
      tipo_zona == "periurbano" & grupo_edad == "10-14" ~ sample(2:7, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "10-14" ~ sample(1:6, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "15-19" ~ sample(8:18, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "15-19" ~ sample(5:15, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "20-34" ~ sample(22:65, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "20-34" ~ sample(12:45, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "35-49" ~ sample(8:22, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "35-49" ~ sample(5:18, n(), replace = TRUE),
      TRUE ~ sample(5:15, n(), replace = TRUE)
    )
  )

# ---------------------------
# 8. Efectos estructurales
# ---------------------------
# Mejor cobertura en rural que en periurbano
# Diferencias por edad leves

efecto_zona <- c(
  "periurbano" = -0.10,
  "rural" = 0.16
)

efecto_edad <- c(
  "10-14" = -0.03,
  "15-19" = -0.01,
  "20-34" = 0.02,
  "35-49" = 0.01
)

# Variación entre barrios moderada
efecto_barrio <- smv_base %>%
  filter(tipo_zona %in% c("periurbano", "rural")) %>%
  mutate(
    efecto_barrio = rnorm(
      n(),
      mean = 0,
      sd = 0.05
    ),
    efecto_barrio = ifelse(
      NAME_2 %in% barrios_priorizados,
      efecto_barrio + 0.10,
      efecto_barrio
    )
  ) %>%
  select(NAME_2, efecto_barrio)

base <- base %>%
  left_join(efecto_barrio, by = "NAME_2")

# ---------------------------
# 9. Disponibilidad temporal del subsidio
# ---------------------------
# No disponible antes de 2018
# Arranca en 2018, crece, presenta un bache leve en pandemia
# y luego se expande con más fuerza

base <- base %>%
  mutate(
    subsidio_disponible = ifelse(anio >= 2018, 1, 0),
    efecto_tiempo = case_when(
      anio < 2018 ~ NA_real_,
      anio == 2018 ~ -1.10,
      anio == 2019 ~ -0.85,
      anio == 2020 ~ -0.95, # leve caída / estancamiento pandemia
      anio == 2021 ~ -0.60, # recuperación
      anio == 2022 ~ -0.25,
      anio == 2023 ~ 0.05,
      anio == 2024 ~ 0.22,
      anio == 2025 ~ 0.38,
      TRUE ~ NA_real_
    )
  )

# ---------------------------
# 10. Función logística
# ---------------------------
inv_logit <- function(x) {
  1 / (1 + exp(-x))
}

# ---------------------------
# 11. Simular cobertura de transporte subsidiado
# ---------------------------
base <- base %>%
  mutate(
    logit_p = ifelse(
      subsidio_disponible == 1,
      0.00 +
        efecto_zona[tipo_zona] +
        efecto_edad[grupo_edad] +
        efecto_barrio +
        efecto_tiempo +
        rnorm(n(), 0, 0.03),
      NA_real_
    ),
    prob_cobertura = ifelse(
      subsidio_disponible == 1,
      inv_logit(logit_p),
      NA_real_
    ),
    prob_cobertura = ifelse(
      subsidio_disponible == 1,
      pmin(pmax(prob_cobertura, 0.08), 0.78),
      NA_real_
    ),
    n_cubiertas = ifelse(
      subsidio_disponible == 1,
      rbinom(n(), size = n_embarazadas, prob = prob_cobertura),
      NA
    ),
    valor = ifelse(
      subsidio_disponible == 1,
      n_cubiertas / n_embarazadas,
      NA_real_
    )
  )

# ---------------------------
# 12. Salidas finales
# ---------------------------

# 12.1 Total municipio objetivo
total_municipio <- base %>%
  group_by(anio) %>%
  summarise(
    valor = ifelse(
      first(anio) < 2018,
      NA_real_,
      sum(n_cubiertas, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE)
    ),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    cod_local = NA_character_,
    sexo = "Mujeres",
    grupo_edad = "Todas las edades",
    zona = "Periurbano y rural"
  ) %>%
  select(iso3, NAME_2, cod_local, anio, sexo, grupo_edad, zona, valor)

# 12.2 Municipio por edad
municipio_edad <- base %>%
  group_by(anio, grupo_edad) %>%
  summarise(
    valor = ifelse(
      first(anio) < 2018,
      NA_real_,
      sum(n_cubiertas, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE)
    ),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    cod_local = NA_character_,
    sexo = "Mujeres",
    zona = "Periurbano y rural"
  ) %>%
  select(iso3, NAME_2, cod_local, anio, sexo, grupo_edad, zona, valor)

# 12.3 Municipio por zona
municipio_zona <- base %>%
  group_by(anio, tipo_zona) %>%
  summarise(
    valor = ifelse(
      first(anio) < 2018,
      NA_real_,
      sum(n_cubiertas, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE)
    ),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    cod_local = NA_character_,
    sexo = "Mujeres",
    grupo_edad = "Todas las edades"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, cod_local, anio, sexo, grupo_edad, zona, valor)

# 12.4 Municipio por zona y edad
municipio_zona_edad <- base %>%
  group_by(anio, tipo_zona, grupo_edad) %>%
  summarise(
    valor = ifelse(
      first(anio) < 2018,
      NA_real_,
      sum(n_cubiertas, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE)
    ),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    cod_local = NA_character_,
    sexo = "Mujeres"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, cod_local, anio, sexo, grupo_edad, zona, valor)

# 12.5 Barrio total
barrio_total <- base %>%
  group_by(anio, NAME_2, tipo_zona) %>%
  summarise(
    valor = ifelse(
      first(anio) < 2018,
      NA_real_,
      sum(n_cubiertas, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE)
    ),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    cod_local = NA_character_,
    sexo = "Mujeres",
    grupo_edad = "Todas las edades"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, cod_local, anio, sexo, grupo_edad, zona, valor)

# 12.6 Barrio por edad
barrio_edad <- base %>%
  group_by(anio, NAME_2, tipo_zona, grupo_edad) %>%
  summarise(
    valor = ifelse(
      first(anio) < 2018,
      NA_real_,
      sum(n_cubiertas, na.rm = TRUE) / sum(n_embarazadas, na.rm = TRUE)
    ),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    cod_local = NA_character_,
    sexo = "Mujeres"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, cod_local, anio, sexo, grupo_edad, zona, valor)

# Unir todo
cobertura_transporte_subsidiado_final <- bind_rows(
  total_municipio,
  municipio_edad,
  municipio_zona,
  municipio_zona_edad,
  barrio_total,
  barrio_edad
) %>%
  arrange(NAME_2, anio, zona, grupo_edad)

# ---------------------------
# 13. Guardar archivos
# ---------------------------
csv_dir <- file.path(output_dir, "csv")
parquet_dir <- file.path(output_dir, "parquet")

if (!dir.exists(csv_dir)) dir.create(csv_dir, recursive = TRUE)
if (!dir.exists(parquet_dir)) dir.create(parquet_dir, recursive = TRUE)

archivo_csv <- file.path(csv_dir, "transport_frequency_municipal.csv")
write_csv(cobertura_transporte_subsidiado_final, archivo_csv)

archivo_parquet <- file.path(parquet_dir, "transport_frequency_municipal.parquet")
write_parquet(cobertura_transporte_subsidiado_final, archivo_parquet)
