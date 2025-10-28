# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# 1) LOAD
companies = (spark.read.option("header", True).option("encoding","ISO-8859-1").option("sep","\t").csv("/Volumes/workspace/default/usecase/companies.txt"))
if len(companies.columns) <= 1:
    companies = (spark.read.option("header", True).option("encoding","ISO-8859-1").csv("/Volumes/workspace/default/usecase/companies.txt"))

investments = (spark.read.option("header", True).option("encoding","ISO-8859-1").csv("/Volumes/workspace/default/usecase/InvestmentData.csv"))
mapping = (spark.read.option("header", True).option("encoding","ISO-8859-1").csv("/Volumes/workspace/default/usecase/mapping.csv"))


# COMMAND ----------

# 2) CLEAN + MASTER_BASE
def first_token(col):
    return F.lower(F.trim(
        F.when(F.instr(col, F.lit("|")) > 0, F.split(col, r"\|")[0])
         .when(F.instr(col, F.lit(",")) > 0, F.split(col, r",")[0])
         .otherwise(col)
    ))

companies_clean = (companies
    .withColumn("permalink", F.lower(F.trim(F.col("permalink"))))
    .withColumn("country_code", F.upper(F.col("country_code"))))

investments_clean = (investments
    .withColumn("company_permalink", F.lower(F.trim(F.col("company_permalink"))))
    .withColumn("funding_round_type", F.lower(F.trim(F.col("funding_round_type"))))
    .withColumn("raised_amount_usd", F.col("raised_amount_usd").cast("double")))

master_base = (investments_clean.alias("i")
    .join(companies_clean.alias("c"), F.col("i.company_permalink")==F.col("c.permalink"), "inner")
    .withColumn("primary_sector", first_token(F.col("c.category_list"))))

# COMMAND ----------

# 3) UNPIVOT mapping.csv dynamically
def sanitize(name: str) -> str:
    import re
    if name is None: return "unnamed_col"
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_")
    return s if s else "unnamed_col"

mapping = mapping.toDF(*[sanitize(c) for c in mapping.columns])

cat_col_actual = None
for c in mapping.columns:
    if c.lower().strip() == "category_list":
        cat_col_actual = c; break
if cat_col_actual is None:
    raise Exception("Could not find 'category_list' column in mapping.csv after sanitizing headers.")

sector_cols = [c for c in mapping.columns if c != cat_col_actual]

mapping_long = (mapping
    .withColumn("category_list", F.lower(F.trim(F.col(cat_col_actual))))
    .withColumn("pairs", F.arrays_zip(F.array(*[F.lit(c) for c in sector_cols]),
                                      F.array(*[F.col(c).cast("string") for c in sector_cols])))
    .withColumn("pair", F.explode("pairs"))
    .select("category_list", F.col("pair.0").alias("main_sector_raw"), F.col("pair.1").alias("flag"))
    .where(F.col("flag").isin("1","1.0","true","True"))
    .withColumn("main_sector", F.lower(F.regexp_replace(F.col("main_sector_raw"), "_", " ")))
    .select("category_list","main_sector").dropDuplicates())


# COMMAND ----------

# 4) MAP + FILTER venture
master = (master_base.alias("mb")
          .join(mapping_long.alias("ml"), F.col("mb.primary_sector")==F.col("ml.category_list"), "left"))\
         .withColumn("main_sector_filled", F.coalesce(F.col("main_sector"), F.lit("others")))

df_venture = master.filter(F.col("funding_round_type")=="venture")



# COMMAND ----------

# 5) Top countries
country_total = df_venture.groupBy("country_code").agg(F.sum("raised_amount_usd").alias("raised_amount_usd")).orderBy(F.desc("raised_amount_usd"))
top9 = country_total.withColumn("raised_amount_usd_million", F.col("raised_amount_usd")/1e6).limit(9)
display(top9)

# COMMAND ----------


# 6) Top3 + sector analysis
top3 = [r["country_code"] for r in top9.select("country_code").limit(4).collect()]
df_top3 = df_venture.filter(F.col("country_code").isin(top3))

sector_analysis = (df_top3.groupBy("country_code", F.col("main_sector_filled").alias("main_sector"))
                   .agg(F.sum("raised_amount_usd").alias("raised_amount_usd"),
                        F.count("*").alias("investment_count"))
                   .orderBy("country_code", F.desc("raised_amount_usd"))
                  ).withColumn("raised_amount_usd_million", F.col("raised_amount_usd")/1e6)

display(sector_analysis)



# COMMAND ----------

# 7) Top sector per country
w = Window.partitionBy("country_code").orderBy(F.desc("raised_amount_usd"))
top_sector_per_country = sector_analysis.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1)\
                                        .select("country_code","main_sector","raised_amount_usd","raised_amount_usd_million")
display(top_sector_per_country)

# COMMAND ----------

# 8) Top funded company per (country, top sector)
w2 = Window.partitionBy("country_code","main_sector").orderBy(F.desc_nulls_last("raised_amount_usd"), F.col("company_permalink"))
base = (df_top3.withColumn("main_sector", F.col("main_sector_filled"))
              .select("country_code","main_sector","company_permalink","raised_amount_usd")
              .withColumn("rn", F.row_number().over(w2)))
top_company_per_sector = base.filter(F.col("rn")==1).select("country_code","main_sector","company_permalink","raised_amount_usd")

top_companies = (top_sector_per_country.alias("t")
                 .join(top_company_per_sector.alias("c"),
                       (F.col("t.country_code")==F.col("c.country_code")) & (F.col("t.main_sector")==F.col("c.main_sector")),
                       "inner")
                 .select(F.col("t.country_code"), F.col("t.main_sector"),
                         F.col("c.company_permalink"), F.col("c.raised_amount_usd")))

# COMMAND ----------

# SHOW RESULTS
display(top9)
display(sector_analysis)
display(top_sector_per_country)
display(top_companies)