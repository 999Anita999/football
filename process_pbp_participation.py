import pandas as pd


class TransformPbPParticipation:

    def __init__(self, new_participation_file, play_hist_file, play_player_no_role_hist_file, play_player_hist_file,
                 today):
        self.new_participation_file = new_participation_file
        self.play_hist_file = play_hist_file
        self.play_player_no_role_hist_file = play_player_no_role_hist_file
        self.play_player_hist_file = play_player_hist_file
        self.today = today

    def add_participation_to_play(self, df_play):
        # Parse the players by df_play data
        # Read in the participation file and drop all rows with no players in the possession team column
        df_part = pd.read_csv(self.new_participation_file, dtype='str')
        df_part = df_part.dropna(subset=['possession_team'])

        # Create play_uuid
        df_part = df_part.rename(columns={'nflverse_game_id': 'game_id'})
        df_part['uuid'] = df_part[['game_id', 'play_id']].agg('_'.join, axis=1)

        # Create additional fields to add to df_play by df_play table - merge by play_uuid
        df_pbp_additional = df_part.filter(['uuid', 'possession_team', 'offense_formation', 'offense_personnel',
                                            'defenders_in_box', 'defense_personnel', 'number_of_pass_rushers',
                                            'n_offense', 'n_defense', 'ngs_air_yards', 'time_to_throw', 'was_pressure',
                                            'route', 'defense_man_zone_type', 'defense_coverage_type',
                                            'offense_players', 'defense_players'])

        df_play_plus = df_play.merge(df_pbp_additional, how='left', on='uuid')

        # Remove the player accomplishments fields, they're not needed here
        df_play_filtered = df_play_plus.drop(df_play_plus.filter(regex='_player_id').columns, axis=1)

        # Concatenate with the history file and save the table
        df_play_hist = pd.read_csv(self.play_hist_file, dtype='str')
        df_play_final = pd.concat([df_play_filtered, df_play_hist])
        df_play_final = df_play_final.reset_index(drop=True)
        df_play_final.to_csv('./production_tables/play.csv', index=False)
        return df_play_plus

    def add_participation_to_play_player(self, df_play_plus, df_play_player):
        # Filter out rows without players identified
        df_play_plus = df_play_plus.query("offense_players.notna()")
        df_play_plus = df_play_plus.rename(columns={'uuid': 'play_uuid'})
        # Filter down df to just the columns I need (to speed up processing)
        df_players_stg = df_play_plus.filter(
            ['play_uuid', 'season_week_id', 'offense_players', 'defense_players', 'posteam', 'defteam'])

        # Create a series that is a list of the offensive and defensive players
        # from the columns that have them all together
        off_players = df_players_stg['offense_players'].str.split(';')
        def_players = df_players_stg['defense_players'].str.split(';')
        # Add those series back into the df
        df_players_stg['off_player'] = off_players
        df_players_stg['def_player'] = def_players
        # Create a row for each player in an offense table and a defense table
        df_offense = df_players_stg.explode('off_player')
        df_defense = df_players_stg.explode('def_player')
        # Add in a field to indicate which side of the ball the player was on
        df_offense['player_side'] = 'offense'
        df_defense['player_side'] = 'defense'
        # Rename columns to prep for concatenating the tables
        df_offense = df_offense.rename(columns={'off_player': 'player', 'posteam': 'team'})
        df_offense = df_offense.drop(columns=['def_player', 'defteam'])
        df_defense = df_defense.rename(columns={'def_player': 'player', 'defteam': 'team'})
        df_defense = df_defense.drop(columns=['off_player', 'posteam'])
        # Concatenate tables to get one table with both the offensive and defensive players
        df_player_participation = pd.concat([df_offense, df_defense])
        df_player_participation = df_player_participation.reset_index(drop=True)
        # Get rid of extra columns
        df_player_participation = df_player_participation.filter(
            ['play_uuid', 'season_week_id', 'player_side', 'player', 'team'])
        df_player_participation = df_player_participation.rename(columns={'player': 'player_id'})
        df_player_participation['season_week_player_id'] = df_player_participation[['season_week_id', 'player_id']].agg(
            '_'.join, axis=1)
        df_player_participation['uuid'] = df_player_participation[['play_uuid', 'player_id']].agg('_'.join, axis=1)
        # Strip down the player file, so you don't duplicate columns
        df_play_player_stripped = df_play_player.filter(['uuid', 'play_uuid'])
        # Create a file of play_players that do not have a role, but were on a play.  I have it separate, instead of in
        #   one file because it was so big that Tableau was choking on it.
        df_play_player_remove = df_player_participation.merge(df_play_player_stripped, how='inner', on='uuid')
        df_play_player_remove = df_play_player_remove.filter(['uuid'])
        df_play_player_remove['remove'] = 'Yes'
        df_play_player_no_role = df_player_participation.merge(df_play_player_remove, how='left', on='uuid')
        df_play_player_no_role = df_play_player_no_role.query('remove!="Yes"')
        df_play_player_no_role = df_play_player_no_role.drop(columns=['remove'])

        df_play_player_no_role_hist = pd.read_csv(self.play_player_no_role_hist_file, dtype='str')
        df_play_player_no_role_final = pd.concat([df_play_player_no_role, df_play_player_no_role_hist])
        df_play_player_no_role_final = df_play_player_no_role_final.reset_index(drop=True)
        df_play_player_no_role_final.to_csv('./production_tables/play_player_no_role.csv', index=False)

        # Create the core play_player table with roles
        df_player_participation = df_player_participation.drop(columns=['player_id', 'season_week_player_id'])
        df_play_player_plus = df_play_player.merge(df_player_participation, how='left', on='uuid')

        df_play_player_hist = pd.read_csv(self.play_player_hist_file, dtype='str')
        df_play_player_final = pd.concat([df_play_player_plus, df_play_player_hist])
        df_play_player_final = df_play_player_final.reset_index(drop=True)
        df_play_player_final.to_csv('./production_tables/play_player.csv', index=False)
