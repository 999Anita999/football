import urllib.request
import datetime as dt
import pytz


def convert_eastern_file_timestamp(timestamp_file):
    with open(timestamp_file) as f:
        newest_file_ts_raw = f.read()
    clean_ts = newest_file_ts_raw.strip()
    newest_file_string = clean_ts[:-4]
    format_date = "%Y-%m-%d %H:%M:%S"
    newest_file_date = dt.datetime.strptime(newest_file_string, format_date)
    eastern_tz = pytz.timezone('America/New_York')
    newest_file_date_eastern = eastern_tz.localize(newest_file_date)
    return newest_file_date_eastern


class Retrieve:

    def __init__(self, this_season, last_checked_ts_file):
        self.this_season = this_season
        self.last_checked_ts_file = last_checked_ts_file
        self.refresh_pbp = False
        self.refresh_pbp_participation = False
        self.refresh_roster = False

    def check_for_new_files(self):
        pbp_timestamp_url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/timestamp.txt'
        urllib.request.urlretrieve(pbp_timestamp_url, "./staging_tables/play_by_play/play_by_play_timestamp.csv")
        play_by_play_timestamp = "staging_tables/play_by_play/play_by_play_timestamp.csv"
        last_nflverse_pbp_update = convert_eastern_file_timestamp(play_by_play_timestamp)

        pbp_participation_timestamp_url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/timestamp.txt'
        urllib.request.urlretrieve(pbp_participation_timestamp_url,
                                   "./staging_tables/participation_by_play/pbp_participation_timestamp.csv")
        pbp_participation_timestamp = "staging_tables/participation_by_play/pbp_participation_timestamp.csv"
        last_nflverse_pbp_participation_update = convert_eastern_file_timestamp(pbp_participation_timestamp)

        roster_timestamp_url = 'https://github.com/nflverse/nflverse-data/releases/download/rosters/timestamp.txt'
        urllib.request.urlretrieve(roster_timestamp_url, "./staging_tables/rosters/roster_weekly_timestamp.csv")
        roster_timestamp = "staging_tables/rosters/roster_weekly_timestamp.csv"
        last_nflverse_roster_update = convert_eastern_file_timestamp(roster_timestamp)

        with open(self.last_checked_ts_file) as f:
            last_refresh = f.read()
        clean_last_refresh = last_refresh.strip()
        format_date = "%Y-%m-%d %H:%M:%S"
        last_refresh_timestamp_naive = dt.datetime.strptime(clean_last_refresh, format_date)
        central_tz = pytz.timezone('America/Chicago')
        last_refresh_timestamp = central_tz.localize(last_refresh_timestamp_naive)
        self.refresh_pbp = last_refresh_timestamp < last_nflverse_pbp_update
        self.refresh_pbp_participation = last_refresh_timestamp < last_nflverse_pbp_participation_update
        self.refresh_roster = last_refresh_timestamp < last_nflverse_roster_update

    def download_new_files(self):
        if self.refresh_roster:
            new_roster_url = f'https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{self.this_season}.csv'
            try:
                urllib.request.urlretrieve(new_roster_url, "./staging_tables/rosters/roster_weekly_new.csv")
            except:
                print('No new roster file')

        if self.refresh_pbp_participation:
            new_part_url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{self.this_season}.csv'
            try:
                urllib.request.urlretrieve(new_part_url,
                                           "./staging_tables/participation_by_play/pbp_participation_new.csv")
            except:
                print('No new pbp participation file')

        if self.refresh_pbp:
            new_pbp_url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{self.this_season}.csv'
            try:
                urllib.request.urlretrieve(new_pbp_url, "./staging_tables/play_by_play/play_by_play_new.csv")
            except:
                print('No new pbp file')
