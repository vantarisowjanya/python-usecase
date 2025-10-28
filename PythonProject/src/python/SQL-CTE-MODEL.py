# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW mapping_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path '/Volumes/workspace/default/usecase/mapping.csv',
# MAGIC   header 'true',
# MAGIC   encoding 'ISO-8859-1'
# MAGIC );

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

# MAGIC %sql
# MAGIC ---------- Paths ----------
# MAGIC CREATE OR REPLACE TEMPORARY VIEW companies_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path '/Volumes/workspace/default/usecase/companies.txt',
# MAGIC   header 'true',
# MAGIC   sep '\t',
# MAGIC   encoding 'ISO-8859-1'
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW investments_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path '/Volumes/workspace/default/usecase/InvestmentData.csv',
# MAGIC   header 'true',
# MAGIC   encoding 'ISO-8859-1'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC WITH
# MAGIC -- 1 Clean Companies Data
# MAGIC companies AS (
# MAGIC   SELECT
# MAGIC     LOWER(TRIM(REGEXP_REPLACE(permalink, '^[ /]+|[ /]+$', ''))) AS permalink,
# MAGIC     category_list,
# MAGIC     UPPER(country_code) AS country_code,
# MAGIC     name,
# MAGIC     homepage_url,
# MAGIC     status,
# MAGIC     state_code,
# MAGIC     region,
# MAGIC     city,
# MAGIC     founded_at
# MAGIC   FROM companies_raw
# MAGIC ),
# MAGIC
# MAGIC -- 2 Clean Investments Data
# MAGIC investments AS (
# MAGIC   SELECT
# MAGIC     LOWER(TRIM(REGEXP_REPLACE(company_permalink, '^[ /]+|[ /]+$', ''))) AS company_permalink,
# MAGIC     funding_round_permalink,
# MAGIC     LOWER(TRIM(funding_round_type)) AS funding_round_type,
# MAGIC     funding_round_code,
# MAGIC     funded_at,
# MAGIC     CAST(raised_amount_usd AS DOUBLE) AS raised_amount_usd
# MAGIC   FROM investments_raw
# MAGIC ),
# MAGIC
# MAGIC -- 3 Join & Build Master Base with Primary Sector
# MAGIC master_base AS (
# MAGIC   SELECT
# MAGIC     i.*,
# MAGIC     c.name,
# MAGIC     c.homepage_url,
# MAGIC     c.category_list,
# MAGIC     c.status,
# MAGIC     c.country_code,
# MAGIC     c.state_code,
# MAGIC     c.region,
# MAGIC     c.city,
# MAGIC     c.founded_at,
# MAGIC     LOWER(TRIM(
# MAGIC       CASE
# MAGIC         WHEN INSTR(c.category_list, '|') > 0 THEN SPLIT(c.category_list, '\\|')[0]
# MAGIC         WHEN INSTR(c.category_list, ',') > 0 THEN SPLIT(c.category_list, ',')[0]
# MAGIC         WHEN INSTR(c.category_list, ';') > 0 THEN SPLIT(c.category_list, ';')[0]
# MAGIC         ELSE c.category_list
# MAGIC       END
# MAGIC     )) AS primary_sector
# MAGIC   FROM investments i
# MAGIC   JOIN (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC       SELECT
# MAGIC         c.*,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC           PARTITION BY LOWER(TRIM(REGEXP_REPLACE(permalink, '^[ /]+|[ /]+$', '')))
# MAGIC           ORDER BY permalink
# MAGIC         ) AS rn
# MAGIC       FROM companies c
# MAGIC     ) x
# MAGIC     WHERE rn = 1
# MAGIC   ) c
# MAGIC   ON i.company_permalink = c.permalink
# MAGIC ),
# MAGIC
# MAGIC -- 4 Map Primary Sector to Main Sector
# MAGIC master AS (
# MAGIC   SELECT
# MAGIC     mb.*,
# MAGIC     coalesce(ml.main_sector,'Others')  AS main_sector
# MAGIC   FROM master_base mb
# MAGIC   LEFT JOIN mapping_long ml
# MAGIC     ON trim(mb.primary_sector) = trim(ml.category_list)
# MAGIC ),
# MAGIC
# MAGIC -- 5 Funding Type Average
# MAGIC funding_type_avg AS (
# MAGIC   SELECT
# MAGIC     funding_round_type,
# MAGIC     AVG(raised_amount_usd) AS avg_raised_amount_usd,
# MAGIC     AVG(raised_amount_usd) / 1e6 AS avg_raised_amount_usd_million
# MAGIC   FROM master
# MAGIC   GROUP BY funding_round_type 
# MAGIC   --6 having avg_raised_amount_usd_million BETWEEN 5 AND 15
# MAGIC ),
# MAGIC
# MAGIC chosen_type AS (
# MAGIC   SELECT COALESCE(
# MAGIC            (SELECT funding_round_type FROM funding_type_avg LIMIT 1),
# MAGIC            'venture'
# MAGIC          ) AS funding_round_type
# MAGIC ),
# MAGIC
# MAGIC -- 7 Filter Master by Chosen Funding Type
# MAGIC df_chosen AS (
# MAGIC   SELECT *
# MAGIC   FROM master
# MAGIC   WHERE funding_round_type IN (SELECT funding_round_type FROM chosen_type) and country_code is not null
# MAGIC ),
# MAGIC
# MAGIC -- 8 Top 3 Countries
# MAGIC top3 AS (
# MAGIC   SELECT
# MAGIC     country_code,
# MAGIC     SUM(raised_amount_usd) AS raised_amount_usd,
# MAGIC     SUM(raised_amount_usd) / 1e6 AS raised_amount_usd_million
# MAGIC   FROM df_chosen
# MAGIC   GROUP BY country_code
# MAGIC   ORDER BY raised_amount_usd DESC
# MAGIC   LIMIT 3
# MAGIC ),
# MAGIC
# MAGIC -- 9 Sector Analysis within Top 3 Countries
# MAGIC sector_analysis AS (
# MAGIC   SELECT
# MAGIC     country_code,
# MAGIC     main_sector,
# MAGIC     COUNT(*) AS investment_count,
# MAGIC     SUM(raised_amount_usd) AS raised_amount_usd,
# MAGIC     SUM(raised_amount_usd) / 1e6 AS raised_amount_usd_million
# MAGIC   FROM df_chosen
# MAGIC   WHERE country_code IN (SELECT country_code FROM top3)
# MAGIC   GROUP BY country_code, main_sector
# MAGIC ),
# MAGIC
# MAGIC -- 10 Top Sector per Country
# MAGIC top_sector_per_country AS (
# MAGIC   SELECT
# MAGIC     country_code,
# MAGIC     main_sector,
# MAGIC     raised_amount_usd,
# MAGIC     raised_amount_usd_million
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       sa.*,
# MAGIC       RANK() OVER (
# MAGIC         PARTITION BY country_code
# MAGIC         ORDER BY investment_count DESC
# MAGIC       ) AS rn
# MAGIC     FROM sector_analysis sa
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC -- 11 Top-Funded Company in Each (Country, Sector)
# MAGIC top_company_per_sector AS (
# MAGIC   SELECT
# MAGIC     country_code,
# MAGIC     main_sector,
# MAGIC     company_permalink,
# MAGIC     raised_amount_usd
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       country_code,
# MAGIC       main_sector,
# MAGIC       company_permalink,
# MAGIC       raised_amount_usd,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY country_code, main_sector
# MAGIC         ORDER BY raised_amount_usd DESC, country_code ASC, company_permalink
# MAGIC       ) AS rn
# MAGIC     FROM df_chosen
# MAGIC     WHERE country_code IN (SELECT country_code FROM top3)
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC
# MAGIC -- 12 FINAL OUTPUT - INVESTABLE COMPANIES
# MAGIC SELECT
# MAGIC   t.country_code,
# MAGIC   c.main_sector,
# MAGIC   c.company_permalink,
# MAGIC   (c.raised_amount_usd / 1e6) AS raised_amount_usd_million
# MAGIC FROM top_sector_per_country t
# MAGIC JOIN top_company_per_sector c
# MAGIC   ON t.country_code = c.country_code
# MAGIC   AND t.main_sector = trim(c.main_sector)
# MAGIC ORDER BY t.country_code, t.main_sector,raised_amount_usd_million DESC;
# MAGIC