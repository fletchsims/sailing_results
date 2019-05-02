from race_results.web_scrape import ScrapeTechScoreResults
import pandas as pd
from lib.constants import local_path
import numpy as np
pd.set_option('display.max_columns', None)

soup = ScrapeTechScoreResults.dev_create_soup_obj(local_path)
page = ScrapeTechScoreResults.extract_raw_results(soup)
df = ScrapeTechScoreResults.raw_results_to_df(page)
df['regatta_id'] = '14_gill_national_round_1_west'
df_clean = df.drop(['note', 'finish_pos', 'blank', 'tot'], axis=1)
df_clean = df_clean.drop(df_clean.index[[0, 1]]).reset_index()
# df_clean['finish_pos'] = df_clean['finish_pos'].ffill(axis=0) # it will only forward fill over NaN or values...these are blank

print(df_clean.head())

# ScrapeTechScoreResults.write_results_to_db(df)