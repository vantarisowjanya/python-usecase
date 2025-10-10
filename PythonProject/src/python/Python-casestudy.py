# Databricks notebook source
#imports and display settings
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 200)

# COMMAND ----------

# 1.Load datasets 
# companies.txt may be tab/pipe/comma separated; adjust sep parameter if required.
companies = pd.read_csv('/Volumes/workspace/default/usecase/companies.txt', sep='\t', encoding='ISO-8859-1', low_memory=False)


investments = pd.read_csv('/Volumes/workspace/default/usecase/InvestmentData.csv', encoding='ISO-8859-1',low_memory=False)
mapping = pd.read_csv('/Volumes/workspace/default/usecase/mapping.csv',encoding='ISO-8859-1', low_memory=False)

companies.head(2)
investments.head(2)
mapping.head(2)


# COMMAND ----------

#2. Normalize column names
companies.columns = companies.columns.str.strip().str.lower()
investments.columns = investments.columns.str.strip().str.lower()
mapping.columns = mapping.columns.str.strip().str.lower()

print('companies cols:', companies.columns.tolist()[:20])
print('investments cols:', investments.columns.tolist()[:20])
print('mapping cols:', mapping.columns.tolist()[:40])


# COMMAND ----------

# 3.Merge datasets
# Ensure company_permalink/permalink matching format: sometimes investments company_permalink includes leading '/'
investments['company_permalink'] = investments['company_permalink'].astype(str).str.lower().str.strip()
companies['permalink'] = companies['permalink'].astype(str).str.lower().str.strip()

master = pd.merge(investments, companies, how='inner',left_on='company_permalink', right_on='permalink', suffixes=('_inv','_comp'))

print('master shape:', master.shape)
master[['company_permalink','permalink','funding_round_type','raised_amount_usd','country_code','category_list']].head(6)

# COMMAND ----------

#4.Extract primary sector from category_list
def extract_primary(cat):
    if pd.isna(cat): return np.nan
    # some files use '|' separator, some use ','; handle both
    if '|' in cat:
        return cat.split('|')[0].strip().lower()
    else:
        return str(cat).split(',')[0].strip().lower()

master['primary_sector'] = master['category_list'].apply(extract_primary)
master['primary_sector'] = master['primary_sector'].replace('', np.nan)
master[['category_list','primary_sector']].head(10)

# COMMAND ----------

# 5.Clean and reshape mapping.csv
# mapping usually has a 'category_list' column and many columns representing main sectors with 0/1 flags.
mapping.columns = mapping.columns.str.strip().str.lower()
if 'category_list' not in mapping.columns:
    # try first column name as category column
    mapping = mapping.rename(columns={mapping.columns[0]:'category_list'})

mapping['category_list'] = mapping['category_list'].astype(str).str.strip().str.lower()

# Melt to long format
value_cols = [c for c in mapping.columns if c!='category_list']
melt = mapping.melt(id_vars=['category_list'], value_vars=value_cols, var_name='main_sector', value_name='flag')

# Keep only flagged rows (flag likely 1 or True)
melt = melt[melt['flag'].astype(str).isin(['1','1.0','True','true'])]
melt = melt[['category_list','main_sector']].drop_duplicates().reset_index(drop=True)

print('mapping (long) sample:')
melt.head(10)


# COMMAND ----------

# 6. Merge master with mapping on primary sector
# Both sides lowercase already
melt['category_list'] = melt['category_list'].str.lower().str.strip()
master['primary_sector'] = master['primary_sector'].str.lower().str.strip()

master = master.merge(melt, how='left', left_on='primary_sector', right_on='category_list')
print('After mapping merge, sample:')
master[['primary_sector','main_sector']].drop_duplicates().head(20)


# COMMAND ----------

# 7. Validate mapping results and inspect unmapped primary sectors
unmapped = master[master['main_sector'].isnull()]['primary_sector'].unique()
print('Number of unmapped primary_sector values:', len(unmapped))
print(unmapped[:50])
# If many unmapped, consider inspecting mapping file or filling manually

# COMMAND ----------

# 8. Choose funding type by average raised amount (range 5M to 15M recommended)
funding_type_avg = (master.groupby('funding_round_type')['raised_amount_usd']
                    .mean().reset_index().sort_values(by='raised_amount_usd', ascending=False))
funding_type_avg['raised_amount_usd_million'] = funding_type_avg['raised_amount_usd']/1e6
funding_type_avg


# COMMAND ----------

# Choose funding type 
# Common recommended: 'venture'
chosen_type = 'venture'
df = master[master['funding_round_type']==chosen_type].copy()
print('Records selected:', df.shape[0])
df[['funding_round_type','raised_amount_usd']].head(3)

# COMMAND ----------

#9.Top countries by total funding (for chosen funding type)
country_total = (df.groupby('country_code')['raised_amount_usd']
                   .sum().reset_index().sort_values(by='raised_amount_usd', ascending=False))
top9 = country_total.head(9).copy()
top9['raised_amount_usd_million'] = top9['raised_amount_usd']/1e6
top9


# COMMAND ----------

#10.Select top 3 countries
top3 = top9['country_code'].head(3).tolist()
print('Top 3 countries:', top3)
df_top3 = df[df['country_code'].isin(top3)].copy()
df_top3.shape

# COMMAND ----------

#11.Sector analysis within top3 countries
sector_analysis = (df_top3.groupby(['country_code','main_sector'])
                     .agg({'raised_amount_usd':'sum','company_permalink':'count'})
                     .rename(columns={'company_permalink':'investment_count'})
                     .reset_index()
                     .sort_values(by=['country_code','raised_amount_usd'], ascending=[True,False]))
sector_analysis['raised_amount_usd_million'] = sector_analysis['raised_amount_usd']/1e6
sector_analysis.head(20)

# COMMAND ----------

#12. Top sector per each top country
top_sector_per_country = sector_analysis.groupby('country_code').first().reset_index()
top_sector_per_country[['country_code','main_sector','raised_amount_usd_million']]


# COMMAND ----------

#13. For each country top sector, find top funded company
def get_top_company(country, sector):
    temp = df_top3[(df_top3['country_code']==country) & (df_top3['main_sector']==sector)]
    if temp.empty:
        return None
    row = temp.loc[temp['raised_amount_usd'].idxmax()]
    return pd.Series({'company_permalink': row['company_permalink'], 'raised_amount_usd': row['raised_amount_usd']})

results = []
for _, r in top_sector_per_country.iterrows():
    country = r['country_code']
    sector = r['main_sector']
    s = get_top_company(country, sector)
    if s is not None:
        s['country_code'] = country
        s['main_sector'] = sector
        results.append(s)
top_companies = pd.DataFrame(results)
top_companies
