# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY VIEW mapping_raw
 USING CSV
 OPTIONS (
   path '/Volumes/workspace/default/usecase/mapping.csv',
   header 'true',
   encoding 'ISO-8859-1'
 );

# COMMAND ----------


table_name = "mapping_raw"

# Get the columns from the table
cols = [r.col_name for r in spark.sql(f"DESCRIBE {table_name}").collect()]
category_col = "category_list"
sector_cols = [c for c in cols if c != category_col]

# Dynamically generate concat_ws() and sector array
concat_expr = "concat_ws(',', " + ", ".join([f"cast(`{c}` as string)" for c in sector_cols]) + ") as flags_csv"
sector_array = "split('" + ",".join(sector_cols) + "', ',') as sectors"

sql_query = f"""
WITH base AS (
SELECT
{category_col},
{concat_expr}
FROM mapping_raw
),
exploded AS (
SELECT
{category_col},
posexplode(split(flags_csv, ',')) AS (pos, flag)
FROM base
),
sector_names AS (
SELECT {sector_array}
)
SELECT
lower(regexp_replace(e.{category_col},'[^A-Za-z0-9 0]','')) AS category_list,
lower(REGEXP_REPLACE(regexp_replace(trim(s.sectors[e.pos]),'[^A-Za-z0-9]',''),'_', ' ')) AS main_sector,
cast(e.flag AS int) AS flag
FROM exploded e
CROSS JOIN sector_names s where e.flag  in ('1') and category_list is not null
ORDER BY e.{category_col}, main_sector"""
spark.sql(sql_query).createOrReplaceTempView("mapping_long")

# COMMAND ----------

 %sql
 ---------- Paths ----------
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



 WITH
 -- 1 Clean Companies Data
 companies AS (
   SELECT
     LOWER(TRIM(REGEXP_REPLACE(permalink, '^[ /]+|[ /]+$', ''))) AS permalink,
     category_list,
     UPPER(country_code) AS country_code,
     name,
     homepage_url,
     status,
     state_code,
     region,
     city,
     founded_at
   FROM companies_raw
 ),

 -- 2 Clean Investments Data
 investments AS (
   SELECT
     LOWER(TRIM(REGEXP_REPLACE(company_permalink, '^[ /]+|[ /]+$', ''))) AS company_permalink,
     funding_round_permalink,
     LOWER(TRIM(funding_round_type)) AS funding_round_type,
     funding_round_code,
     funded_at,
     CAST(raised_amount_usd AS DOUBLE) AS raised_amount_usd
   FROM investments_raw
 ),

 -- 3 Join & Build Master Base with Primary Sector
 master_base AS (
   SELECT
     i.*,
     c.name,
     c.homepage_url,
     c.category_list,
     c.status,
     c.country_code,
     c.state_code,
     c.region,
     c.city,
     c.founded_at,
     LOWER(TRIM(
       CASE
         WHEN INSTR(c.category_list, '|') > 0 THEN SPLIT(c.category_list, '\\|')[0]
         WHEN INSTR(c.category_list, ',') > 0 THEN SPLIT(c.category_list, ',')[0]
         WHEN INSTR(c.category_list, ';') > 0 THEN SPLIT(c.category_list, ';')[0]
         ELSE c.category_list
       END
     )) AS primary_sector
   FROM investments i
   JOIN (
     SELECT *
     FROM (
       SELECT
         c.*,
         ROW_NUMBER() OVER (
           PARTITION BY LOWER(TRIM(REGEXP_REPLACE(permalink, '^[ /]+|[ /]+$', '')))
           ORDER BY permalink
         ) AS rn
       FROM companies c
     ) x
     WHERE rn = 1
   ) c
   ON i.company_permalink = c.permalink
 ),

 -- 4 Map Primary Sector to Main Sector
 master AS (
   SELECT
     mb.*,
     coalesce(ml.main_sector,'Others')  AS main_sector
   FROM master_base mb
   LEFT JOIN mapping_long ml
     ON trim(mb.primary_sector) = trim(ml.category_list)
 ),

 -- 5 Funding Type Average
 funding_type_avg AS (
   SELECT
     funding_round_type,
     AVG(raised_amount_usd) AS avg_raised_amount_usd,
     AVG(raised_amount_usd) / 1e6 AS avg_raised_amount_usd_million
   FROM master
   GROUP BY funding_round_type 
   --6 having avg_raised_amount_usd_million BETWEEN 5 AND 15
 ),

 chosen_type AS (
   SELECT COALESCE(
            (SELECT funding_round_type FROM funding_type_avg LIMIT 1),
            'venture'
          ) AS funding_round_type
 ),

 -- 7 Filter Master by Chosen Funding Type
 df_chosen AS (
   SELECT *
   FROM master
   WHERE funding_round_type IN (SELECT funding_round_type FROM chosen_type) and country_code is not null
 ),

 -- 8 Top 3 Countries
 top3 AS (
   SELECT
     country_code,
     SUM(raised_amount_usd) AS raised_amount_usd,
     SUM(raised_amount_usd) / 1e6 AS raised_amount_usd_million
   FROM df_chosen
   GROUP BY country_code
   ORDER BY raised_amount_usd DESC
   LIMIT 3
 ),

 -- 9 Sector Analysis within Top 3 Countries
 sector_analysis AS (
   SELECT
     country_code,
     main_sector,
     COUNT(*) AS investment_count,
     SUM(raised_amount_usd) AS raised_amount_usd,
     SUM(raised_amount_usd) / 1e6 AS raised_amount_usd_million
   FROM df_chosen
   WHERE country_code IN (SELECT country_code FROM top3)
   GROUP BY country_code, main_sector
 ),

 -- 10 Top Sector per Country
 top_sector_per_country AS (
   SELECT
     country_code,
     main_sector,
     raised_amount_usd,
     raised_amount_usd_million
   FROM (
     SELECT
       sa.*,
       RANK() OVER (
         PARTITION BY country_code
         ORDER BY investment_count DESC
       ) AS rn
     FROM sector_analysis sa
   )
   WHERE rn = 1
 ),
 -- 11 Top-Funded Company in Each (Country, Sector)
 top_company_per_sector AS (
   SELECT
     country_code,
     main_sector,
     company_permalink,
     raised_amount_usd
   FROM (
     SELECT
       country_code,
       main_sector,
       company_permalink,
       raised_amount_usd,
       ROW_NUMBER() OVER (
         PARTITION BY country_code, main_sector
         ORDER BY raised_amount_usd DESC, country_code ASC, company_permalink
       ) AS rn
     FROM df_chosen
     WHERE country_code IN (SELECT country_code FROM top3)
   )
   WHERE rn = 1
 )

 -- 12 FINAL OUTPUT - INVESTABLE COMPANIES
 SELECT
   t.country_code,
   c.main_sector,
   c.company_permalink,
   (c.raised_amount_usd / 1e6) AS raised_amount_usd_million
 FROM top_sector_per_country t
 JOIN top_company_per_sector c
   ON t.country_code = c.country_code
   AND t.main_sector = trim(c.main_sector)
 ORDER BY t.country_code, t.main_sector,raised_amount_usd_million DESC;
