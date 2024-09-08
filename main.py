import pandas as pd
import fnmatch
import urllib.request
import datetime as dt
import os
from retrieve_new_files import Retrieve
from process_roster import TransformRoster
from process_pbp import TransformPbP

# Create a variable for last season and this season
today = dt.date.today()
month = today.month
if month > 7:
    last_season = today.year - 1
    this_season = today.year
else:
    last_season = today.year - 2
    this_season = today.year - 1

# # Download new files
# retrieve_files = Retrieve(this_season)
# retrieve_files.download_new_files()
#
# #------------process new roster file------------------
# new_roster_file = "./staging_tables/rosters/roster_weekly_new.csv"
# hist_roster_file = "./staging_tables/rosters/roster_weekly_hist.csv"
# try:
#     transform_roster = TransformRoster(new_roster_file, hist_roster_file, today, last_season)
#     df_prepped_new_roster = transform_roster.prep_new_roster()
#     transform_roster.merge_roster(df_prepped_new_roster)
# except FileNotFoundError:
#     print("No changes to roster table")
#
# finally:
#     if os.path.exists("./staging_tables/rosters/roster_weekly_new.csv"):
#         os.remove("./staging_tables/rosters/roster_weekly_new.csv")

#------------process new pbp file------------------
new_pbp_file = "./staging_tables/play_by_play/play_by_play_new.csv"
hist_pbp_file = "./staging_tables/play_by_play/play_by_play_hist.csv"

transform_pbp = TransformPbP(new_pbp_file, hist_pbp_file, today, last_season)
df_prepped_new_pbp = transform_pbp.prep_new_pbp()

df_game = transform_pbp.create_game_table(df_prepped_new_pbp)
transform_pbp.create_game_team_table(df_game)

transform_pbp.create_drive_table(df_prepped_new_pbp)

transform_pbp.create_series_table(df_prepped_new_pbp)


# print(df_prepped_new_pbp.head())
