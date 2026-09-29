#' Calculate absolute and relative gaps between groups
#'
#' A shared utility for computing health equity gaps (absolute difference
#' and ratio) between two strata — e.g. male vs female, urban vs rural.
#' Used by all indicator scripts across the monorepo.
#'
#' @param data Data frame containing the indicator data
#' @param var_estrato Unquoted column name of stratification var. (e.g. sexo)
#' @param var_valor Unquoted column name of the numeric value (e.g. valor)
#' @param grupo_ref Reference group label (e.g. "Femenino")
#' @param grupo_comp Comparison group label (e.g. "Masculino")
#' @param var_anio Optional unquoted column name for the year variable
#' @param var_territorio Optional unquoted column name for territory
#' @param territorios Optional character vector to filter specific territories
#' @param fun_resumen Summary function to aggregate values (default: mean)
#' @return Data frame with columns for each group, brecha_absoluta, and razon
#' @export
#' @examples
#' calcular_brechas(
#'   data = suicidio,
#'   var_estrato = sexo,
#'   var_valor = valor,
#'   grupo_ref = "Femenino",
#'   grupo_comp = "Masculino",
#'   var_anio = anio,
#'   var_territorio = territorio
#' )
calcular_brechas <- function(data,
                             var_estrato,
                             var_valor,
                             grupo_ref,
                             grupo_comp,
                             var_anio = NULL,
                             var_territorio = NULL,
                             territorios = NULL,
                             fun_resumen = mean) {
  var_estrato <- rlang::enquo(var_estrato)
  var_valor <- rlang::enquo(var_valor)
  var_anio <- rlang::enquo(var_anio)
  var_territorio <- rlang::enquo(var_territorio)

  datos <- data |>
    dplyr::filter(
      !is.na(!!var_estrato),
      !is.na(!!var_valor),
      !!var_estrato %in% c(grupo_ref, grupo_comp)
    )

  if (!is.null(territorios) && !rlang::quo_is_null(var_territorio)) {
    datos <- datos |>
      dplyr::filter(!!var_territorio %in% territorios)
  }

  vars_group <- list()
  if (!rlang::quo_is_null(var_anio)) vars_group <- c(vars_group, list(var_anio))
  if (!rlang::quo_is_null(var_territorio)) vars_group <- c(vars_group, list(var_territorio))
  vars_group <- c(vars_group, list(var_estrato))

  resultado <- datos |>
    dplyr::group_by(!!!vars_group) |>
    dplyr::summarise(
      valor_resumen = fun_resumen(!!var_valor, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      names_from  = !!var_estrato,
      values_from = valor_resumen
    ) |>
    dplyr::mutate(
      brecha_absoluta = .data[[grupo_comp]] - .data[[grupo_ref]],
      razon = dplyr::if_else(
        is.na(.data[[grupo_ref]]) | is.na(.data[[grupo_comp]]) |
          .data[[grupo_ref]] <= 0 | .data[[grupo_comp]] <= 0,
        NA_real_,
        .data[[grupo_comp]] / .data[[grupo_ref]]
      )
    )

  if (!rlang::quo_is_null(var_anio) && !rlang::quo_is_null(var_territorio)) {
    resultado <- resultado |> dplyr::arrange(!!var_territorio, !!var_anio)
  } else if (!rlang::quo_is_null(var_anio)) {
    resultado <- resultado |> dplyr::arrange(!!var_anio)
  } else if (!rlang::quo_is_null(var_territorio)) {
    resultado <- resultado |> dplyr::arrange(!!var_territorio)
  }

  return(resultado)
}
