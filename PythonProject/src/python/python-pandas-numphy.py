# Databricks notebook source
import pandas as pd
import numpy as np

# ---------- Paths ----------
COMPANIES_PATH  = "/Volumes/workspace/default/usecase/companies.txt"
INVEST_PATH     = "/Volumes/workspace/default/usecase/InvestmentData.csv"
MAPPING_PATH    = "/Volumes/workspace/default/usecase/mapping.csv"


# COMMAND ----------

# ---------- 1) Load data ----------
companies = pd.read_csv(COMPANIES_PATH, sep='\t', encoding='ISO-8859-1')
if companies.shape[1] == 1:
    # Retry if not tab-delimited
    companies = pd.read_csv(COMPANIES_PATH, encoding='ISO-8859-1')

investments = pd.read_csv(INVEST_PATH, encoding='ISO-8859-1')
mapping = pd.read_csv(MAPPING_PATH, encoding='ISO-8859-1')

print("Data loaded:")
print(f"Companies: {companies.shape}, Investments: {investments.shape}, Mapping: {mapping.shape}")

# COMMAND ----------

investments

# COMMAND ----------

def clean_permalink(col):
    return col.astype(str).str.strip().str.lower().str.replace(r'^[ /]+|[ /]+$', '', regex=True)

companies['permalink'] = clean_permalink(companies['permalink'])
companies['country_code'] = companies['country_code'].astype(str).str.upper()
companies.drop_duplicates(subset='permalink', inplace=True)

investments['company_permalink'] = clean_permalink(investments['company_permalink'])
investments['funding_round_type'] = investments['funding_round_type'].astype(str).str.strip().str.lower()
investments['raised_amount_usd'] = pd.to_numeric(investments['raised_amount_usd'], errors='coerce')

# COMMAND ----------

def extract_primary_sector(x):
    if pd.isna(x): return np.nan
    for sep in ['|', ',', ';']:
        if sep in x: return x.split(sep)[0].strip().lower()
    return x.strip().lower()

companies['primary_sector'] = companies['category_list'].apply(extract_primary_sector)

master_base = pd.merge(investments, companies[['permalink','country_code','primary_sector']],
                       left_on='company_permalink', right_on='permalink', how='inner')

# COMMAND ----------

# ---------- 4) Unpivot mapping.csv -> mapping_long ----------
mapping_long = (mapping.melt(id_vars=['category_list'], var_name='main_sector_raw', value_name='flag')
                .query("flag in [1, 1.0, '1', '1.0', 'true', 'True']")
                .assign(main_sector=lambda df: df['main_sector_raw'].str.replace('_', ' ', regex=False).str.lower())
                [['category_list','main_sector']]
                .drop_duplicates())

mapping_long['category_list'] = mapping_long['category_list'].str.strip().str.lower()

# COMMAND ----------

# ---------- 5) Map primary_sector -> main_sector ----------
master = pd.merge(master_base, mapping_long, left_on='primary_sector', right_on='category_list', how='left')
master['main_sector'] = master['main_sector'].fillna('others')

# COMMAND ----------

# ---------- 6) Funding type average filter ----------
funding_avg = (master.groupby('funding_round_type', as_index=False)['raised_amount_usd']
               .mean().rename(columns={'raised_amount_usd':'avg_raised_amount_usd'}))
funding_avg['avg_raised_amount_usd_million'] = funding_avg['avg_raised_amount_usd']/1e6

#print("\nAverage funding by type (million USD):")
#print(funding_avg.sort_values('avg_raised_amount_usd_million', ascending=False).head())

chosen = funding_avg.query('5 <= avg_raised_amount_usd_million <= 15')
#chosen_type = chosen['funding_round_type'].iloc[0] if not chosen.empty else 'venture'
chosen_type = 'venture'

#print(f" Chosen funding type: {chosen_type}")

df_chosen = master[master['funding_round_type'] == chosen_type].copy()
funding_avg

# COMMAND ----------

# ---------- 7) Top 9 -> Top 3 countries ----------
top9 = (df_chosen.groupby('country_code', as_index=False)['raised_amount_usd']
        .sum().sort_values('raised_amount_usd', ascending=False).head(9))
top3 = top9['country_code'].head(4).tolist()
print(f"Top 3 countries: {top3}")

df_top3 = df_chosen[df_chosen['country_code'].isin(top3)].copy()

# COMMAND ----------

# ---------- 8) Sector analysis within Top 3 ----------
sector_analysis = (df_top3.groupby(['country_code','main_sector'], as_index=False)
                   .agg(raised_amount_usd=('raised_amount_usd','sum'),
                        investment_count=('raised_amount_usd','size')))
sector_analysis['raised_amount_usd_million'] = sector_analysis['raised_amount_usd']/1e6

sector_analysis

# COMMAND ----------

# ---------- 9) Top sector per country ----------
sector_analysis['rank'] = sector_analysis.groupby('country_code')['raised_amount_usd'].rank(ascending=False, method='first')
top_sector_per_country = sector_analysis[sector_analysis['rank']==1].copy()

# COMMAND ----------

# ---------- 10) Top company per (country, sector) ----------
df_top3 = df_top3[df_top3['country_code'] != 'NAN']
df_top3['rank'] = df_top3.groupby(['country_code','main_sector'])['raised_amount_usd'].rank(ascending=False, method='first')
top_company_per_sector = df_top3[df_top3['rank']==1][['country_code','main_sector','company_permalink','raised_amount_usd']]

# COMMAND ----------

final = pd.merge(
    top_sector_per_country[['country_code', 'main_sector', 'raised_amount_usd']],
    top_company_per_sector,
    on=['country_code', 'main_sector'],
    how='inner'
).sort_values(['country_code', 'main_sector'])

print("\n Final Result:")
#final = final[['country_code', 'main_sector', 'company_permalink', 'raised_amount_usd']]
#print(final.head(10))
final=final[['country_code', 'main_sector', 'company_permalink', 'raised_amount_usd_y']].rename(columns={'raised_amount_usd_y':'raised_amount_usd'})
final['raised_amount_usd_million']=(final['raised_amount_usd']/1e6).round(3).astype(float)
final= final[['country_code','main_sector','company_permalink','raised_amount_usd_million']]
# ---------- 11)
final.head(10)

# COMMAND ----------

# ---------- 12) Save result ----------
final.to_csv("final_investment_result.csv", index=False)
print("\n Saved as final_investment_result.csv")