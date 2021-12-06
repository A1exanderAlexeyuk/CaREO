#' Identify drug regimen exposures
#'
#' This function will create a new table in an OMOP CDM database that contains drug regimens exposures for each person.
#' Regimens are defined continuous periods of time where one or more ingredients taken within 30 days of each other.
#'
#' @details
#' This algorithm is based largely on OHDSI's drug era logic (https://ohdsi.github.io/CommonDataModel/sqlScripts.html#Drug_Eras).
#' The major difference is that instead of creating a different era for each ingredient the regimen finder creates eras for combinations
#' of ingredients and matches them to user specified regimens (i.e. ingredient combinations).
#'
#' Ingredients that are not part of any regimen are completely ignored by the algorithm.
#' The first step is to roll up drug exposures to the RxNorm ingredient level.
#' Then considering only ingredients that are part of at least one regimen in the user's input the algorithm
#' creates exposure eras with 30 day collapsing logic that ignore ingredient. These eras are continuous periods of exposure to any ingredient in at least one regimen.
#' Next the algorithm identifies all ingredients exposures that occur within an each exposure era.
#' If the complete set of ingredients in an era matches the set of ingredients in a regimen definition then we have identified a regimen exposure
#' and a new record will be created in the final regimen table.
#'
#' This function should work on any suppported OHDSI database platform.
#' @importFrom magrittr %>%
#' @param con A DatabaseConnectorJdbcConnection object
#' @param regimenIngredient A dataframe that contains the regimen definitions
#' @param cdmDatabaseSchema The schema containing an OMOP CDM in the database
#' @param writeDatabaseSchema The name of the schema where the results should be saved in the database. Write access is required. If NULL (default) then result will be written to a temp table.
#' @param regimenTableName The name of the results table that will contain the regimens
#'
#' @return Returns NULL. This function is called for its side effect of creating a regimen table in the tempEmulationSchema database
#' @export
#'
#' @examples
#'
#' library(Eunomia)
#' # create or derive a dataframe that defines regimens
#' regimenIngredient <- data.frame(
#'   regimen_name = c("Venetoclax and Obinutuzumab", "Venetoclax and Obinutuzumab", "Doxycycline monotherapy"),
#'   regimen_id = c(35100084L, 35100084L, 35806103),
#'   ingredient_name = c("venetoclax", "obinutuzumab", "Doxycycline"),
#'   ingredient_concept_id = c(35604205L, 44507676L, 1738521)
#' )
#'
#' cd <- getEunomiaConnectionDetails()
#' con <- connect(cd)
#' createRegimens(con, regimenIngredient, "main", "main", "myregimens")
#'
#' # download the result from the database
#' regimens <- dbGetQuery(con, "select * from myregimens")
#'
#'


createRegimens <- function(con,
                           cdmDatabaseSchema,
                           writeDatabaseSchema = NULL,
                           regimenTableName = "regimen",
                           tempEmulationSchema) {
  regimenIngredient <- getRegimenIngredient()
  # verify input
  stopifnot(
    is.data.frame(regimenIngredient),
    names(regimenIngredient) == c(
      "regimen_name",
      "regimen_id",
      "ingredient_name",
      "ingredient_concept_id"
    )
  )

  if (con@dbms %in% c("bigquery", "oracle") & Sys.getenv("sqlRenderTempEmulationSchema") == "") {
    rlang::abort("sqlRenderTempEmulationSchema environment variable must be set when using bigquery or oracle.")
  }
  rlang::inform("Loading regimenIngredient into the database.")
  DatabaseConnector::insertTable(con,
    tableName = "regimenIngredient",
    data = regimenIngredient,
    tempTable = TRUE,
    dropTableIfExists = TRUE,
    tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema"),
    progressBar = TRUE
  )
  check <- dbGetQuery(con, SqlRender::translate("SELECT * FROM #regimenIngredient",
    con@dbms,
    tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema")
  ))
  if (nrow(regimenIngredient) != nrow(check)) rlang::abort("regimenIngredient was not uploaded to the database.")


  rlang::inform("Calculating regimens.")
  sqlFileName <- "RegimenCreation.sql"
  pathToSql <- system.file("sql", sqlFileName, package = getThisPackageName())
  sql <- readChar(pathToSql, file.info(pathToSql)$size)
  DatabaseConnector::renderTranslateExecuteSql(
    con = con,
    sql = sql,
    cdm_database_schema = cdmDatabaseSchema,
    ingredient_ids = regimenIngredient$ingredient_concept_id,
    regimenTableName = regimenTableName,
    tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema")
  )

  sql <- SqlRender::render("SELECT COUNT(*) as n FROM #@regimenTableName",
    regimenTableName = regimenTableName
  )
  sql <- SqlRender::translate(sql, con@dbms, tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema"))
  n <- DatabaseConnector::dbGetQuery(con, sql)$n
  if (n == 0) warning("0 regimens found")

  if (!is.null(writeDatabaseSchema)) {
    sql <- "
    DROP TABLE IF EXISTS @writeDatabaseSchema.@regimenTableName;

    SELECT
      drug_era_id
      ,person_id
      ,regimen_start_date
      ,regimen_end_date
      ,regimen_id
      ,regimen_name
    INTO @writeDatabaseSchema.@regimenTableName
    FROM #regimenIngredientEra;"
    tryCatch(
      DatabaseConnector::renderTranslateExecuteSql(con, sql,
        regimenTableName = regimenTableName,
        writeDatabaseSchema = writeDatabaseSchema,
        tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema")
      ),
      error = function(e) {
        message(paste0("Regimen table with ", n, " rows saved as temporary table named ", regimenTableName))
        warning(paste0("Writing regimen table to ", writeDatabaseSchema, ".", regimenTableName, " failed"))
        warning(e)
      }
    )
    # might check that the schema exists first and user has write access
    message(paste0("Regimen table with ", n, " rows saved to ", writeDatabaseSchema, ".", regimenTableName))
  } else {
    message(paste0("Regimen table with ", n, " rows saved as temporary table named ", regimenTableName))
  }
  invisible(NULL)
}
