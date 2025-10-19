-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW companies_raw
USING CSV
OPTIONS (
  path '/Volumes/workspace/default/usecase/companies.txt',
  header 'true',
  sep '\t',
  encoding 'ISO-8859-1'
);

CREATE OR REPLACE TEMPORARY VIEW investments_raw
USING CSV
OPTIONS (
  path '/Volumes/workspace/default/usecase/InvestmentData.csv',
  header 'true',
  encoding 'ISO-8859-1'
);

CREATE OR REPLACE TEMPORARY VIEW mapping_raw
USING CSV
OPTIONS (
  path '/Volumes/workspace/default/usecase/mapping.csv',
  header 'true',
  encoding 'ISO-8859-1'
);

-- COMMAND ----------

select * from investments_raw

-- COMMAND ----------

-- ==========================================
-- 2) CLEAN KEYS & BUILD COMPANIES/INVESTMENTS
-- ==========================================

CREATE OR REPLACE TEMPORARY VIEW companies AS
SELECT
  lower(trim(regexp_replace(permalink, '^[ /]+|[ /]+$', ''))) AS permalink,
  category_list,
  upper(country_code) AS country_code,
  name, homepage_url, status, state_code, region, city, founded_at
FROM companies_raw;

CREATE OR REPLACE TEMPORARY VIEW investments AS
SELECT
  lower(trim(regexp_replace(company_permalink, '^[ /]+|[ /]+$', ''))) AS company_permalink,
  funding_round_permalink,
  lower(trim(funding_round_type)) AS funding_round_type,
  funding_round_code,
  funded_at,
  CAST(raised_amount_usd AS DOUBLE) AS raised_amount_usd
FROM investments_raw;


-- COMMAND ----------

select * from investments

-- COMMAND ----------

-- ==================================
-- 3) MASTER BASE (JOIN + PRIMARY SECTOR)
-- ==================================

CREATE OR REPLACE TEMPORARY VIEW master_base AS
SELECT
  i.*,
  c.name, c.homepage_url, c.category_list, c.status, c.country_code,
  c.state_code, c.region, c.city, c.founded_at,
  lower(trim(
    CASE
      WHEN instr(c.category_list, '|') > 0 THEN split(c.category_list, '\\|')[0]
      WHEN instr(c.category_list, ',') > 0 THEN split(c.category_list, ',')[0]
      WHEN instr(c.category_list, ';') > 0 THEN split(c.category_list, ';')[0]
      ELSE c.category_list
    END
  )) AS primary_sector
FROM investments i
JOIN (
  SELECT * FROM (
    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY lower(trim(regexp_replace(permalink, '^[ /]+|[ /]+$', ''))) ORDER BY permalink) rn
    FROM companies c
  ) x WHERE rn = 1
) c
ON i.company_permalink = c.permalink;

-- COMMAND ----------


-- ====================================
-- 4) UNPIVOT mapping.csv → mapping_long
-- ====================================

CREATE OR REPLACE TEMPORARY VIEW mapping_long AS
SELECT lower(trim(category_list)) AS category_list,
       lower(regexp_replace(main_sector_raw, '_', ' ')) AS main_sector
FROM (
  SELECT category_list,
         stack(
           9,
           'automotive & sports',                    `Automotive & Sports`,
           'blanks',                                 Blanks,
           'cleantech / semiconductors',             `Cleantech / Semiconductors`,
           'entertainment',                          Entertainment,
           'health',                                 Health,
           'manufacturing',                          Manufacturing,
           'news, search and messaging',             `News, Search and Messaging`,
           'others',                                 Others,
           'social, finance, analytics, advertising',`Social, Finance, Analytics, Advertising`
         ) AS (main_sector_raw, flag)
  FROM mapping_raw
) s
WHERE CAST(flag AS STRING) IN ('1','1.0','true','True');


-- COMMAND ----------

-- ======================================
-- 5) MAP primary_sector → main_sector
-- ======================================

CREATE OR REPLACE TEMPORARY VIEW master AS
SELECT mb.*,
       COALESCE(ml.main_sector, 'others') AS main_sector
FROM master_base mb
LEFT JOIN mapping_long ml
  ON mb.primary_sector = ml.category_list;


-- COMMAND ----------

-- Compute funding type averages
CREATE OR REPLACE TEMPORARY VIEW funding_type_avg AS
SELECT funding_round_type,
       AVG(raised_amount_usd) AS avg_raised_amount_usd,
       AVG(raised_amount_usd)/1e6 AS avg_raised_amount_usd_million
FROM master
GROUP BY funding_round_type
ORDER BY avg_raised_amount_usd DESC;

-- Filter funding types by average
CREATE OR REPLACE TEMPORARY VIEW funding_type_filtered AS
SELECT * FROM funding_type_avg
WHERE avg_raised_amount_usd_million BETWEEN 5 AND 15;

-- Choose a funding type, fallback to 'venture' if none found
CREATE OR REPLACE TEMPORARY VIEW chosen_type AS
WITH first_type AS (
  SELECT funding_round_type FROM funding_type_filtered LIMIT 1
)
SELECT funding_round_type FROM first_type
UNION ALL
SELECT 'venture' WHERE NOT EXISTS (SELECT 1 FROM first_type);

-- COMMAND ----------

-- ===============================
-- 7) FILTER TO CHOSEN FUNDING TYPE
-- ===============================

CREATE OR REPLACE TEMPORARY VIEW df_chosen AS
SELECT * FROM master
WHERE funding_round_type IN (SELECT funding_round_type FROM chosen_type);

-- COMMAND ----------

-- ============================
-- 8) TOP 9 COUNTRIES (TOTAL USD)
-- ============================

CREATE OR REPLACE TEMPORARY VIEW top9 AS
SELECT country_code,
       SUM(raised_amount_usd) AS raised_amount_usd,
       SUM(raised_amount_usd)/1e6 AS raised_amount_usd_million
FROM df_chosen
GROUP BY country_code
ORDER BY raised_amount_usd DESC
LIMIT 9;

-- COMMAND ----------

-- ============================
-- 9) TOP 3 COUNTRIES
-- ============================

CREATE OR REPLACE TEMPORARY VIEW top3 AS
SELECT country_code FROM top9 ORDER BY raised_amount_usd DESC LIMIT 4;

-- COMMAND ----------

-- ==================================
-- 10) SECTOR ANALYSIS WITHIN TOP 3
-- ==================================

CREATE OR REPLACE TEMPORARY VIEW sector_analysis AS
SELECT
  country_code,
  main_sector,
  COUNT(*) AS investment_count,
  SUM(raised_amount_usd) AS raised_amount_usd,
  SUM(raised_amount_usd)/1e6 AS raised_amount_usd_million
FROM df_chosen
WHERE country_code IN (SELECT country_code FROM top3)
GROUP BY country_code, main_sector
ORDER BY country_code, raised_amount_usd DESC;

-- COMMAND ----------

select * from sector_analysis

-- COMMAND ----------

-- ===================================
-- 11) TOP SECTOR PER EACH TOP COUNTRY
-- ===================================

CREATE OR REPLACE TEMPORARY VIEW top_sector_per_country AS
SELECT country_code, main_sector, raised_amount_usd, raised_amount_usd_million
FROM (
  SELECT sa.*,
         Rank() OVER (PARTITION BY country_code ORDER BY raised_amount_usd DESC, main_sector) AS rn
  FROM sector_analysis sa
)
WHERE rn = 1;


-- COMMAND ----------

select * from top_sector_per_country

-- COMMAND ----------

-- ===================================================
-- 12) TOP-FUNDED COMPANY IN EACH (COUNTRY, TOP SECTOR)
-- ===================================================

CREATE OR REPLACE TEMPORARY VIEW top_company_per_sector AS
SELECT country_code, main_sector, company_permalink, raised_amount_usd
FROM (
  SELECT
    country_code,
    main_sector,
    company_permalink,
    raised_amount_usd,
    ROW_NUMBER() OVER (
      PARTITION BY country_code, main_sector
      ORDER BY raised_amount_usd DESC,country_code asc, company_permalink
    ) AS rn
  FROM df_chosen
  WHERE country_code IN (SELECT country_code FROM top3)
)
WHERE rn = 1;

-- COMMAND ----------

select * from top_company_per_sector

-- COMMAND ----------

select * from top_company_per_sector order by country_code asc, raised_amount_usd desc;

-- COMMAND ----------

-- =====================
-- 13) FINAL INVESTABLES
-- =====================

SELECT
  t.country_code,
  t.main_sector,
  c.company_permalink,
  (c.raised_amount_usd)/1e6 AS raised_amount_usd_million
FROM top_sector_per_country t
JOIN top_company_per_sector c
  ON t.country_code = c.country_code AND t.main_sector = c.main_sector
ORDER BY t.country_code, t.main_sector