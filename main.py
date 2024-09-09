import pandas as pd
import fnmatch
import urllib.request
import datetime as dt
import os
from retrieve_new_files import Retrieve
from process_roster import TransformRoster
from process_pbp import TransformPbP
from process_pbp_participation import TransformPbPParticipation

# Create a variable for last season and this season
today = dt.date.today()
month = today.month
if month > 7:
    last_season = today.year - 1
    this_season = today.year
else:
    last_season = today.year - 2
    this_season = today.year - 1

# Download new files
# retrieve_files = Retrieve(this_season)
# retrieve_files.download_new_files()

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
#
#------------process new pbp file------------------
new_pbp_file = "./staging_tables/play_by_play/play_by_play_new.csv"
# game_hist_file = "./staging_tables/play_by_play/game_hist.csv"
# game_team_hist_file = "./staging_tables/play_by_play/game_team_hist.csv"
# drive_hist_file = "./staging_tables/play_by_play/drive_hist.csv"
# series_hist_file = "./staging_tables/play_by_play/series_hist.csv"
# play_hist_file = "./staging_tables/play_by_play/play_hist.csv"
# play_player_hist_file = "./staging_tables/play_by_play/play_player_hist.csv"


try:
    transform_pbp = TransformPbP(new_pbp_file, today, last_season)
    #  game_hist_file, game_team_hist_file, play_hist_file, play_player_hist_file,
    df_prepped_new_pbp = transform_pbp.prep_new_pbp()

    df_game = transform_pbp.create_game_table(df_prepped_new_pbp)
    transform_pbp.create_game_team_table(df_game)

    transform_pbp.create_drive_table(df_prepped_new_pbp)

    transform_pbp.create_series_table(df_prepped_new_pbp)

    df_play = transform_pbp.create_play_staging_df(df_prepped_new_pbp)
    df_play_player = transform_pbp.create_play_player_roles_staging_df(df_play)

except FileNotFoundError:
    print("No changes to play_by_play tables")

else:
    # ------------process new participation file------------------
    new_participation_file = "./staging_tables/participation_by_play/pbp_participation_new.csv"
    play_hist_file = "./staging_tables/play_by_play/play_hist.csv"
    play_player_hist_file = "./staging_tables/play_by_play/play_player_hist.csv"

    try:
        transform_pbp_participation = TransformPbPParticipation(new_participation_file, today)
        # play_hist_file, play_player_hist_file,
        df_play_plus = transform_pbp_participation.add_participation_to_play(df_play)
        transform_pbp_participation.add_participation_to_play_player(df_play_plus, df_play_player)

    except FileNotFoundError:
        print('No new pbp participation file, so none processed')

# finally:
#     if os.path.exists("./staging_tables/play_by_play/play_by_play_new.csv"):
#         os.remove("./staging_tables/play_by_play/play_by_play_new.csv")







