# =========================================================
# San Martín del Valle - Simulación de empleo informal
# Proporción de personas con empleo informal o sin protección social, por barrio
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
library(arrow)

# ---------------------------
# 3. Semilla
# ---------------------------
set.seed(2468)

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
grupos_edad <- c("15-19", "20-34", "35-49", "50-64", "65 y más")
sexos <- c("Mujeres", "Hombres")

# Barrios con mayor vulnerabilidad laboral
barrios_criticos <- c(
  "La Cañada",
  "El Mirador",
  "Los Pinos",
  "Nueva Esperanza"
)

# ---------------------------
# 6. Base barrio-año-edad-sexo
# ---------------------------
base <- expand_grid(
  anio = anios,
  NAME_2 = smv_base$NAME_2,
  grupo_edad = grupos_edad,
  sexo = sexos
) %>%
  left_join(smv_base, by = "NAME_2")

# ---------------------------
# 7. Población simulada por celda
# ---------------------------
base <- base %>%
  mutate(
    n_personas = case_when(
      tipo_zona == "urbano" & grupo_edad == "15-19" ~ sample(35:90, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "15-19" ~ sample(25:70, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "15-19" ~ sample(15:50, n(), replace = TRUE),
      tipo_zona == "urbano" & grupo_edad == "20-34" ~ sample(90:220, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "20-34" ~ sample(65:170, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "20-34" ~ sample(35:110, n(), replace = TRUE),
      tipo_zona == "urbano" & grupo_edad == "35-49" ~ sample(85:210, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "35-49" ~ sample(60:160, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "35-49" ~ sample(30:100, n(), replace = TRUE),
      tipo_zona == "urbano" & grupo_edad == "50-64" ~ sample(55:150, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "50-64" ~ sample(40:120, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "50-64" ~ sample(20:75, n(), replace = TRUE),
      tipo_zona == "urbano" & grupo_edad == "65 y más" ~ sample(25:90, n(), replace = TRUE),
      tipo_zona == "periurbano" & grupo_edad == "65 y más" ~ sample(18:65, n(), replace = TRUE),
      tipo_zona == "rural" & grupo_edad == "65 y más" ~ sample(10:45, n(), replace = TRUE),
      TRUE ~ sample(25:100, n(), replace = TRUE)
    )
  ) %>%
  mutate(
    n_personas = round(n_personas * runif(n(), 0.95, 1.05)),
    n_personas = pmax(n_personas, 10)
  )

# ---------------------------
# 8. Efectos estructurales
# ---------------------------
# Gradiente esperado:
# urbano < periurbano < rural

efecto_zona <- c(
  "urbano" = -0.30,
  "periurbano" = 0.10,
  "rural" = 0.42
)

# Mayor informalidad en jóvenes y personas mayores
efecto_edad <- c(
  "15-19" = 0.34,
  "20-34" = 0.05,
  "35-49" = 0.00,
  "50-64" = 0.12,
  "65 y más" = 0.25
)

# Diferencia por sexo
efecto_sexo <- c(
  "Mujeres" = 0.07,
  "Hombres" = 0.00
)

# Variabilidad intra-zona por barrio
efecto_barrio <- smv_base %>%
  mutate(
    efecto_barrio = rnorm(
      n(),
      mean = case_when(
        tipo_zona == "urbano" ~ -0.04,
        tipo_zona == "periurbano" ~ 0.03,
        tipo_zona == "rural" ~ 0.08,
        TRUE ~ 0
      ),
      sd = 0.10
    ),
    efecto_barrio = ifelse(
      NAME_2 %in% barrios_criticos,
      efecto_barrio + 0.18,
      efecto_barrio
    )
  ) %>%
  select(NAME_2, efecto_barrio)

base <- base %>%
  left_join(efecto_barrio, by = "NAME_2")

# ---------------------------
# 9. Patrón temporal
# ---------------------------
# Ligera mejora pre-pandemia,
# quiebre fuerte en 2020-2021,
# y aumento leve sostenido post-pandemia

base <- base %>%
  mutate(
    efecto_tiempo = case_when(
      anio == 2016 ~ 0.00,
      anio == 2017 ~ -0.02,
      anio == 2018 ~ -0.04,
      anio == 2019 ~ -0.06,
      anio == 2020 ~ 0.18,
      anio == 2021 ~ 0.24,
      anio == 2022 ~ 0.26,
      anio == 2023 ~ 0.28,
      anio == 2024 ~ 0.30,
      anio == 2025 ~ 0.32,
      TRUE ~ 0.00
    )
  )

# Impacto más fuerte en periurbano y rural durante y después de pandemia
base <- base %>%
  mutate(
    efecto_tiempo = efecto_tiempo + case_when(
      anio >= 2020 & tipo_zona == "periurbano" ~ 0.03,
      anio >= 2020 & tipo_zona == "rural" ~ 0.06,
      TRUE ~ 0
    )
  )

# Efecto adicional en mujeres durante y después de pandemia
base <- base %>%
  mutate(
    efecto_tiempo = efecto_tiempo + case_when(
      anio >= 2020 & sexo == "Mujeres" ~ 0.03,
      TRUE ~ 0
    )
  )

# ---------------------------
# 10. Función logística
# ---------------------------
inv_logit <- function(x) {
  1 / (1 + exp(-x))
}

# ---------------------------
# 11. Simular proporción de empleo informal
# ---------------------------
base <- base %>%
  mutate(
    logit_p = -0.20 +
      efecto_zona[tipo_zona] +
      efecto_edad[grupo_edad] +
      efecto_sexo[sexo] +
      efecto_barrio +
      efecto_tiempo +
      rnorm(n(), 0, 0.10),
    prob_informal = inv_logit(logit_p),
    prob_informal = pmin(pmax(prob_informal, 0.05), 0.95),
    n_informal = rbinom(n(), size = n_personas, prob = prob_informal),
    valor = n_informal / n_personas
  )

# ---------------------------
# 12. Salidas finales
# ---------------------------

# 12.1 Total municipio
total_municipio <- base %>%
  group_by(anio) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    sexo = "Ambos sexos",
    grupo_edad = "Todas las edades",
    zona = "Total"
  ) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.2 Municipio por sexo
municipio_sexo <- base %>%
  group_by(anio, sexo) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    grupo_edad = "Todas las edades",
    zona = "Total"
  ) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.3 Municipio por edad
municipio_edad <- base %>%
  group_by(anio, grupo_edad) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    sexo = "Ambos sexos",
    zona = "Total"
  ) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.4 Municipio por zona
municipio_zona <- base %>%
  group_by(anio, tipo_zona) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    sexo = "Ambos sexos",
    grupo_edad = "Todas las edades"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.5 Municipio por sexo y edad
municipio_sexo_edad <- base %>%
  group_by(anio, sexo, grupo_edad) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    zona = "Total"
  ) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.6 Municipio por zona y sexo
municipio_zona_sexo <- base %>%
  group_by(anio, tipo_zona, sexo) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    grupo_edad = "Todas las edades"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.7 Municipio por zona y edad
municipio_zona_edad <- base %>%
  group_by(anio, tipo_zona, grupo_edad) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
    sexo = "Ambos sexos"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.8 Municipio por zona, sexo y edad
municipio_zona_sexo_edad <- base %>%
  group_by(anio, tipo_zona, sexo, grupo_edad) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    NAME_2 = "San Martín del Valle",
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.9 Barrio total
barrio_total <- base %>%
  group_by(anio, NAME_2, tipo_zona) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    sexo = "Ambos sexos",
    grupo_edad = "Todas las edades"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.10 Barrio por sexo
barrio_sexo <- base %>%
  group_by(anio, NAME_2, tipo_zona, sexo) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    grupo_edad = "Todas las edades"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.11 Barrio por edad
barrio_edad <- base %>%
  group_by(anio, NAME_2, tipo_zona, grupo_edad) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL",
    sexo = "Ambos sexos"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# 12.12 Barrio por sexo y edad
barrio_sexo_edad <- base %>%
  group_by(anio, NAME_2, tipo_zona, sexo, grupo_edad) %>%
  summarise(
    valor = sum(n_informal, na.rm = TRUE) / sum(n_personas, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    iso3 = "COL"
  ) %>%
  rename(zona = tipo_zona) %>%
  select(iso3, NAME_2, anio, sexo, grupo_edad, zona, valor)

# Unir todo
empleo_informal_final <- bind_rows(
  total_municipio,
  municipio_sexo,
  municipio_edad,
  municipio_zona,
  municipio_sexo_edad,
  municipio_zona_sexo,
  municipio_zona_edad,
  municipio_zona_sexo_edad,
  barrio_total,
  barrio_sexo,
  barrio_edad,
  barrio_sexo_edad
) %>%
  arrange(NAME_2, anio, zona, sexo, grupo_edad)

# ---------------------------
# 13. Guardar archivos
# ---------------------------

csv_dir <- file.path(output_dir, "csv")
parquet_dir <- file.path(output_dir, "parquet")

if (!dir.exists(csv_dir)) dir.create(csv_dir, recursive = TRUE)
if (!dir.exists(parquet_dir)) dir.create(parquet_dir, recursive = TRUE)

archivo_csv <- file.path(csv_dir, "informal_employment_municipal.csv")
write_csv(empleo_informal_final, archivo_csv)

archivo_parquet <- file.path(parquet_dir, "informal_employment_municipal.parquet")
write_parquet(empleo_informal_final, archivo_parquet)
