import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import datetime as dt
import os
from retrieve_new_files import Retrieve
# from pytz import timezone
import pytz


df_roster_new = pd.read_csv('staging_tables/rosters/roster_weekly_new.csv', dtype='str')

# Filter down to the fields I need
df_roster_new = df_roster_new.filter(
    ['season', 'week', 'gsis_id', 'full_name', 'team', 'position', 'depth_chart_position', 'jersey_number',
     'status', 'full_name', 'birth_date', 'height', 'weight', 'college', 'entry_year',
     'rookie_year', 'draft_club', 'draft_number'])

# Rename gsis_id to player_id for consistency
df_roster_new = df_roster_new.rename(columns={'gsis_id': 'player_id', })


# Find just the latest week
ser = pd.Series(df_roster_new['week'], dtype='int32')
latest = str(ser.max())
print(latest)

# Add flag to indicate that these are the records from the latest update
df_roster_new['newest'] = np.where(df_roster_new['week'] == latest, '1','')

# Add season_week_player_id which is the primary key and what I need to join it into play_player
df_roster_new['week_2'] = df_roster_new['week'].astype('str').str.zfill(2)
df_roster_new['player_id'] = df_roster_new['player_id'].astype('str')
df_roster_new['id'] = df_roster_new[['season', 'week_2', 'player_id']].agg('_'.join, axis=1)
df_roster_new = df_roster_new.drop(columns=['week_2'])

# Add season_team_id, so I can join in season_team file
df_roster_new['season_team_id'] = df_roster_new['season'].astype('str') + '_' + df_roster_new[
    'team'].astype(
    'str')

# Rearrange the columns
df_roster_new = df_roster_new.filter(
    ['last_update', 'newest', 'id', 'season_team_id', 'player_id', 'season', 'week', 'team', 'full_name', 'position',
     'depth_chart_position', 'jersey_number', 'status', 'full_name', 'birth_date', 'height', 'weight',
     'college', 'entry_year', 'rookie_year', 'draft_club', 'draft_number']
)

df_roster_new = df_roster_new.reset_index(drop=True)

df_roster_new.to_csv('staging_tables/rosters/roster_weekly_newest.csv', index=False)