 /****
  DRUG ERA
  Note: Eras derived from DRUG_EXPOSURE table, using 30d gap.
  Era collapsing logic copied and modified from https://ohdsi.github.io/CommonDataModel/sqlScripts.html#Drug_Eras
   ****/
  DROP TABLE IF EXISTS #cteDrugTarget;

  /* / */

  -- Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date, or add days supply, or add 1 day to the start date
  SELECT d.DRUG_EXPOSURE_ID
      ,d.PERSON_ID
      ,c.CONCEPT_ID AS INGREDIENT_CONCEPT_ID
      ,d.DRUG_TYPE_CONCEPT_ID
      ,DRUG_EXPOSURE_START_DATE
      ,COALESCE(DRUG_EXPOSURE_END_DATE, DATEADD(day, DAYS_SUPPLY, DRUG_EXPOSURE_START_DATE),
      DATEADD(day, 1, DRUG_EXPOSURE_START_DATE)) AS DRUG_EXPOSURE_END_DATE
  INTO #cteDrugTarget
  FROM @cdm_database_schema.DRUG_EXPOSURE d
  INNER JOIN @cdm_database_schema.CONCEPT_ANCESTOR ca ON ca.DESCENDANT_CONCEPT_ID = d.DRUG_CONCEPT_ID
  INNER JOIN @cdm_database_schema.CONCEPT c ON ca.ANCESTOR_CONCEPT_ID = c.CONCEPT_ID
  WHERE c.DOMAIN_ID = 'Drug'
      AND c.CONCEPT_CLASS_ID = 'Ingredient'
      AND c.CONCEPT_ID IN(@ingredient_ids);

  /* / */

  DROP TABLE IF EXISTS #cteEndDates;

  /* / */

  SELECT PERSON_ID
      ,DATEADD(day, - 30, EVENT_DATE) AS END_DATE -- unpad the end date
  INTO #cteEndDates
  FROM (
      SELECT E1.PERSON_ID
          ,E1.EVENT_DATE
          ,COALESCE(E1.START_ORDINAL, MAX(E2.START_ORDINAL)) START_ORDINAL
          ,E1.OVERALL_ORD
      FROM (
          SELECT PERSON_ID
              ,EVENT_DATE
              ,EVENT_TYPE
              ,START_ORDINAL
              ,ROW_NUMBER() OVER (
                  PARTITION BY PERSON_ID ORDER BY EVENT_DATE, EVENT_TYPE
                  ) AS OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
          FROM (
              -- select the start dates, assigning a row number to each
              SELECT PERSON_ID
                  ,DRUG_EXPOSURE_START_DATE AS EVENT_DATE
                  ,0 AS EVENT_TYPE
                  ,ROW_NUMBER() OVER (
                      PARTITION BY PERSON_ID ORDER BY DRUG_EXPOSURE_START_DATE
                      ) AS START_ORDINAL
              FROM #cteDrugTarget

              UNION ALL

              -- add the end dates with NULL as the row number, padding the end dates by 30 to allow a grace period for overlapping ranges.
              SELECT PERSON_ID
                  ,DATEADD(day, 30, DRUG_EXPOSURE_END_DATE)
                  ,1 AS EVENT_TYPE
                  ,NULL
              FROM #cteDrugTarget
              ) RAWDATA
          ) E1
      INNER JOIN (
          SELECT PERSON_ID
              ,DRUG_EXPOSURE_START_DATE AS EVENT_DATE
              ,ROW_NUMBER() OVER (
                  PARTITION BY PERSON_ID ORDER BY DRUG_EXPOSURE_START_DATE
                  ) AS START_ORDINAL
          FROM #cteDrugTarget
          ) E2 ON E1.PERSON_ID = E2.PERSON_ID
          AND E2.EVENT_DATE <= E1.EVENT_DATE
      GROUP BY E1.PERSON_ID
          ,E1.EVENT_DATE
          ,E1.START_ORDINAL
          ,E1.OVERALL_ORD
      ) E
  WHERE 2 * E.START_ORDINAL - E.OVERALL_ORD = 0;

  /* / */

  DROP TABLE IF EXISTS #cteDrugExpEnds;

  /* / */

  SELECT d.PERSON_ID
      ,d.DRUG_TYPE_CONCEPT_ID
      ,d.DRUG_EXPOSURE_START_DATE
      ,MIN(e.END_DATE) AS ERA_END_DATE
  INTO #cteDrugExpEnds
  FROM #cteDrugTarget d
  INNER JOIN #cteEndDates e ON d.PERSON_ID = e.PERSON_ID
      AND e.END_DATE >= d.DRUG_EXPOSURE_START_DATE
  GROUP BY d.PERSON_ID
      ,d.DRUG_TYPE_CONCEPT_ID
      ,d.DRUG_EXPOSURE_START_DATE;

  /* / */

  DROP TABLE IF EXISTS #exposureEra;

  SELECT
    row_number() OVER (ORDER BY person_id) AS drug_era_id
    ,person_id
    ,era_start_date
    ,era_end_date
  INTO #exposureEra
  FROM (
    SELECT
    person_id
    ,min(DRUG_EXPOSURE_START_DATE) AS era_start_date
    ,ERA_END_DATE as era_end_date
    FROM #cteDrugExpEnds
    GROUP BY person_id
      ,drug_type_concept_id
      ,ERA_END_DATE
  );

  -- Add ingredients to eras
  DROP TABLE IF EXISTS #comboIngredientEras;

  SELECT DISTINCT
    e.drug_era_id
    ,e.person_id as person_id
    ,e.era_start_date
    ,e.era_end_date
    ,i.INGREDIENT_CONCEPT_ID AS ingredient_concept_id
  INTO #comboIngredientEras
  FROM
  #exposureEra e
  LEFT JOIN #cteDrugTarget i
    ON e.person_id = i.PERSON_ID
    AND i.DRUG_EXPOSURE_START_DATE >= e.era_start_date
    AND i.DRUG_EXPOSURE_START_DATE <= e.era_end_date;

  -- Match comination ingredient eras with regimens
  -- If an exposure era has the same ingredients as a regimen we have a match
  DROP TABLE IF EXISTS #regimenIngredientEra;

  SELECT
    drug_era_id
    ,person_id
    ,era_start_date as regimen_start_date
    ,era_end_date as regimen_end_date
    ,ingredient_concept_id
    ,regimen_id
    ,regimen_name
  INTO #regimenIngredientEra
  FROM (
    SELECT DISTINCT
      e.*,
      r.regimen_name,
      r.regimen_id,
      r.ingredient_name AS regimen_ingredient,
      r.num_ingredients_in_regimen,
      COUNT(e.ingredient_concept_id) OVER(PARTITION BY drug_era_id, regimen_id) AS num_ingredients_in_intersection
    FROM #comboIngredientEras e
    INNER JOIN (
      SELECT *, COUNT(ingredient_concept_id) OVER(PARTITION BY regimen_id) AS num_ingredients_in_regimen from #regimenIngredient
    ) r
    ON e.ingredient_concept_id = r.ingredient_concept_id
  ) cte
  WHERE num_ingredients_in_regimen = num_ingredients_in_intersection;

  DROP TABLE IF EXISTS #@regimenTableName;

  SELECT DISTINCT
    drug_era_id
    ,person_id
    ,regimen_start_date
    ,regimen_end_date
    ,regimen_id
    ,regimen_name
  INTO #@regimenTableName
  FROM #regimenIngredientEra;
