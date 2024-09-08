import urllib.request


class Retrieve:

    def __init__(self, this_season):
        self.this_season = this_season

    def download_new_files(self):
        new_roster_url = f'https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{self.this_season}.csv'
        try:
            urllib.request.urlretrieve(new_roster_url, "./staging_tables/rosters/roster_weekly_new.csv")
        except:
            print('No new roster file')

        new_part_url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{self.this_season}.csv'
        try:
            urllib.request.urlretrieve(new_part_url, "./staging_tables/participation_by_play/pbp_participation_new.csv")
        except:
            print('No new pbp participation file')

        new_pbp_url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{self.this_season}.csv'
        try:
            urllib.request.urlretrieve(new_pbp_url, "./staging_tables/play_by_play/play_by_play_new.csv")
        except:
            print('No new pbp file')
