import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime as dt
import os
from retrieve_new_files import Retrieve
# from pytz import timezone
import pytz


# def convert_eastern_file_timestamp(timestamp_file):
#     with open(timestamp_file) as f:
#         newest_file_ts_raw = f.read()
#     clean_ts = newest_file_ts_raw.strip()
#     newest_file_string = clean_ts[:-4]
#     format_date = "%Y-%m-%d %H:%M:%S"
#     newest_file_date = dt.datetime.strptime(newest_file_string, format_date)
#     eastern_tz = pytz.timezone('America/New_York')
#     newest_file_date_eastern = eastern_tz.localize(newest_file_date)
#     return newest_file_date_eastern
#
#
# play_by_play_timestamp = "staging_tables/play_by_play/play_by_play_timestamp.csv"
# last_nflverse_pbp_update = convert_eastern_file_timestamp(play_by_play_timestamp)
#
# pbp_participation_timestamp = "staging_tables/participation_by_play/pbp_participation_timestamp.csv"
# last_nflverse_pbp_participation_update = convert_eastern_file_timestamp(pbp_participation_timestamp)
#
# roster_timestamp = "staging_tables/rosters/roster_weekly_timestamp.csv"
# last_nflverse_roster_update = convert_eastern_file_timestamp(roster_timestamp)
#
# with open('last_checked.txt') as f:
#     last_refresh = f.read()
# clean_last_refresh = last_refresh.strip()
# format_date = "%Y-%m-%d %H:%M:%S"
# last_refresh_timestamp_naive = dt.datetime.strptime(clean_last_refresh, format_date)
# central_tz = pytz.timezone('America/Chicago')
# last_refresh_timestamp = central_tz.localize(last_refresh_timestamp_naive)
# refresh_pbp = last_refresh_timestamp < last_nflverse_pbp_update
# refresh_pbp_participation = last_refresh_timestamp < last_nflverse_pbp_participation_update
# refresh_roster = last_refresh_timestamp < last_nflverse_roster_update
# print(refresh_pbp)
# print(refresh_pbp_participation)
# print(refresh_roster)
