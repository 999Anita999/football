import pandas as pd
import numpy as np


class TransformRoster:

    def __init__(self, new_roster_file, hist_roster_file, today, last_season):
        self.new_roster_file = new_roster_file
        self.hist_roster_file = hist_roster_file
        self.last_season = last_season
        self.today = today

    def prep_new_roster(self):
        df_roster_new = pd.read_csv(self.new_roster_file, dtype='str')

        # Filter down to the fields I need
        df_roster_new = df_roster_new.filter(
            ['season', 'week', 'gsis_id', 'full_name', 'team', 'position', 'depth_chart_position', 'jersey_number',
             'status', 'full_name', 'birth_date', 'height', 'weight', 'college', 'entry_year',
             'rookie_year', 'draft_club', 'draft_number'])

        # Rename gsis_id to player_id for consistency
        df_roster_new = df_roster_new.rename(columns={'gsis_id': 'player_id', })

        # Add update timestamp
        df_roster_new['last_update'] = self.today

        # Find just the latest week
        ser = pd.Series(df_roster_new['week'], dtype='int32')
        latest = str(ser.max())

        # Add flag to indicate that these are the records from the latest update
        df_roster_new['newest'] = np.where(df_roster_new['week'] == latest, '1', '')

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
        return df_roster_new

    def merge_roster(self, df_roster_new):
        df_roster_hist = pd.read_csv(self.hist_roster_file, dtype='str')
        last_update = max(df_roster_hist['last_update'].astype('datetime64[ns]'))
        # Annual check - include only players who played in last 2 seasons to cut file size
        if last_update.month < 9:
            # Find the last season that a player appears
            df_roster_recent = df_roster_hist.groupby('player_id').max('season')
            df_roster_recent = df_roster_recent.filter(['player_id', 'season'])

            # Create a flag called "recent" for the players whose last season was within the last 2
            df_roster_recent['recent'] = self.last_season <= df_roster_recent['season']
            df_roster_recent = df_roster_recent.loc[df_roster_recent.recent]
            df_roster_recent['recent'] = 'Yes'
            df_roster_recent = df_roster_recent.filter(['player_id', 'recent'])

            # Get that flag on the history file and drop the records without anything in 'recent'
            df_roster_hist_merged = df_roster_hist.merge(df_roster_recent, how='left', on='player_id')
            df_roster_hist_merged = df_roster_hist_merged.dropna(subset=['recent'])
            df_roster_hist = df_roster_hist_merged
        df_roster = pd.concat([df_roster_new, df_roster_hist])
        df_roster = df_roster.reset_index(drop=True)
        df_roster.to_csv('./production_tables/roster.csv', index=False)
