#' @importFrom magrittr %>%
getThisPackageName <- function() {
  return("CaREO")
}

getRegimenIngredient <- function() {
  path <- system.file("csv", sql_filename = "regimenIngredients.csv", package = "CaREO", mustWork = TRUE)
  regimenIngredient <- readr::read_csv(path, col_types = "cici") %>%
  dplyr::rename(regimen_id = regimen_concept_id)
  return(regimenIngredient)
}

