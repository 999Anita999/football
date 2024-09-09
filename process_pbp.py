import pandas as pd
import fnmatch
import numpy as np


def create_keys(df_play_by_play):
    df_play_by_play.drive = df_play_by_play.drive.fillna('')
    df_play_by_play['drive_id'] = df_play_by_play[['game_id', 'drive']].agg('_'.join, axis=1)
    df_play_by_play['series_id'] = df_play_by_play[['drive_id', 'series']].agg('_'.join, axis=1)
    df_play_by_play['uuid'] = df_play_by_play[['game_id', 'play_id']].agg('_'.join, axis=1)
    df_play_by_play['week2'] = df_play_by_play['week'].str.zfill(2)
    df_play_by_play['season_week_id'] = df_play_by_play[['season', 'week2']].agg('_'.join, axis=1)


def convert_result(df_game):
    if df_game['result'] == 0:
        return 0.5
    elif df_game['result'] >= 0:
        return 1
    else:
        return 0


def convert_win(df_away):
    if df_away['result'] == 0:
        return 0.5
    elif df_away['result'] >= 0:
        return 0
    else:
        return 1


class TransformPbP:

    def __init__(self, new_pbp_file, game_hist_file, game_team_hist_file, drive_hist_file, series_hist_file,
                 play_hist_file, play_player_hist_file, today, last_season):
        self.new_pbp_file = new_pbp_file
        self.game_hist_file = game_hist_file
        self.game_team_hist_file = game_team_hist_file
        self.drive_hist_file = drive_hist_file
        self.series_hist_file = series_hist_file
        self.play_hist_file = play_hist_file
        self.play_player_hist_file = play_player_hist_file
        self.today = today
        self.last_season = last_season

    def prep_new_pbp(self):
        df_pbp = pd.read_csv(self.new_pbp_file, dtype='str')
        create_keys(df_pbp)
        df_pbp['last_update'] = self.today
        # Create a list of the columns I want
        non_player_pbp_columns_used = ['last_update', 'uuid', 'game_id', 'drive_id', 'series_id', 'season_week_id',
                                       'drive', 'series', 'play_id', 'home_team',
                                       'away_team', 'season_type', 'week', 'game_date', 'season', 'stadium', 'location',
                                       'result', 'total', 'spread_line', 'total_line', 'div_game', 'roof', 'surface',
                                       'home_coach', 'away_coach', 'stadium_id', 'game_stadium', 'away_score',
                                       'home_score', 'home_win', 'drive', 'posteam', 'posteam_type', 'defteam',
                                       'fixed_drive', 'fixed_drive_result', 'drive_real_start_time', 'drive_play_count',
                                       'drive_time_of_possession', 'drive_first_downs', 'drive_inside20',
                                       'drive_ended_with_score', 'drive_quarter_start', 'drive_quarter_end',
                                       'drive_yards_penalized', 'drive_start_transition', 'drive_end_transition',
                                       'drive_game_clock_start', 'drive_game_clock_end', 'drive_start_yard_line',
                                       'drive_play_id_started', 'drive_play_id_ended', 'series_success',
                                       'series_result', 'field_goal_result', 'extra_point_result',
                                       'two_point_conv_result', 'passing_yards', 'receiving_yards', 'rushing_yards',
                                       'lateral_receiving_yards', 'lateral_rushing_yards', 'penalty_type',
                                       'penalty_yards', 'return_yards', 'quarter_seconds_remaining',
                                       'half_seconds_remaining', 'game_seconds_remaining', 'game_half', 'quarter_end',
                                       'drive', 'sp', 'qtr', 'down', 'goal_to_go', 'time', 'yrdln', 'ydstogo', 'ydsnet',
                                       'desc', 'play_type', 'yards_gained', 'shotgun', 'no_huddle', 'qb_dropback',
                                       'qb_kneel', 'qb_spike', 'qb_scramble', 'pass_length', 'pass_location',
                                       'air_yards', 'yards_after_catch', 'run_location', 'run_gap', 'field_goal_result',
                                       'kick_distance', 'first_down']
        # I want all the columns that end in player_id, so here I grab that using a wildcard
        pbp_columns = df_pbp.columns.to_list()
        player_id_pbp_columns_used = fnmatch.filter(pbp_columns, '*player_id')
        # Combine the two lists and filter the pbp dataframe down to just those
        columns_used = non_player_pbp_columns_used + player_id_pbp_columns_used
        df_play_by_play = df_pbp.filter(columns_used)

        return df_play_by_play

    def create_game_table(self, df_play_by_play):
        # Filter down to the columns I need and clean it up
        df_game = df_play_by_play.filter(
            ['last_update', 'game_id', 'season_week_id', 'home_team', 'away_team', 'season_type', 'week',
             'game_date', 'season', 'stadium', 'location', 'result', 'total', 'spread_line',
             'total_line', 'div_game', 'roof', 'surface', 'home_coach', 'away_coach', 'stadium_id',
             'game_stadium', 'away_score', 'home_score'])
        df_game = df_game.drop_duplicates()
        df_game = df_game.rename(columns={'game_id': 'id'})

        # Use the 'result' column to create a column saying if it was a home team win
        df_game['result'] = df_game['result'].astype('float')
        df_game['home_win'] = df_game.apply(convert_result, axis=1)

        df_game_hist = pd.read_csv(self.game_hist_file, dtype='str')
        df_game_final = pd.concat([df_game, df_game_hist])
        df_game_final = df_game_final.reset_index(drop=True)
        df_game_final.to_csv('./production_tables/game.csv', index=False)
        return df_game

    def create_game_team_table(self, df_game):
        # Creating a table that has a record for every game for every team with infor for that team
        df_game = df_game.rename(columns={'id': 'game_id'})

        # Home team
        df_home = df_game.filter(
            ['game_id', 'season_week_id', 'home_team', 'away_team', 'home_score', 'away_score', 'home_win', 'result',
             'season'])
        df_home = df_home.rename(columns={'home_team': 'team', 'away_team': 'opponent', 'away_score': 'opponent_score',
                                          'home_score': 'team_score', 'home_win': 'team_win',
                                          'result': 'point_differential'})
        df_home['home_away'] = 'home'

        # Away team
        df_away = df_game.filter(
            ['game_id', 'season_week_id', 'away_team', 'home_team', 'away_score', 'home_score', 'home_win', 'result',
             'season'])
        # Since 'team_win' is a win for the home team, flip it for the away team
        df_away['team_win'] = df_away.apply(convert_win, axis=1)
        df_away = df_away.rename(columns={'away_team': 'team', 'home_team': 'opponent', 'home_score': 'opponent_score',
                                          'away_score': 'team_score'})
        # Since 'point_differential' is for the home team (originally 'result'), flip it for the away team
        df_away['point_differential'] = df_away['result'] * -1
        df_away = df_away.drop(['result', 'home_win'], axis=1)
        df_away['home_away'] = 'away'

        # Combine the two into one df
        df_team = pd.concat([df_home, df_away])
        df_team = df_team.reset_index(drop=True)

        # Add season_week_team_id which will be the uuid on this table and a season_team foreign key for team data
        df_team['id'] = df_team[['season_week_id', 'team']].agg('_'.join, axis=1)
        df_team['season_team_id'] = df_team[['season', 'team']].agg('_'.join, axis=1)

        df_game_team_hist = pd.read_csv(self.game_team_hist_file, dtype='str')
        df_game_team_final = pd.concat([df_team, df_game_team_hist])
        df_game_team_final = df_game_team_final.reset_index(drop=True)
        df_game_team_final.to_csv('./production_tables/game_team.csv', index=False)

    def create_drive_table(self, df_play_by_play):
        df_drive = df_play_by_play.filter(
            ['game_id', 'drive_id', 'drive', 'posteam', 'posteam_type', 'defteam', 'fixed_drive',
             'fixed_drive_result', 'drive_real_start_time', 'drive_play_count',
             'drive_time_of_possession', 'drive_first_downs', 'drive_inside20',
             'drive_ended_with_score', 'drive_quarter_start', 'drive_quarter_end',
             'drive_yards_penalized', 'drive_start_transition', 'drive_end_transition',
             'drive_game_clock_start', 'drive_game_clock_end', 'drive_start_yard_line',
             'drive_play_id_started', 'drive_play_id_ended'])
        df_drive = df_drive.rename(columns={'drive_id': 'id'})
        df_drive = df_drive.drop_duplicates()
        df_drive = df_drive.reset_index(drop=True)

        df_drive_hist = pd.read_csv(self.drive_hist_file, dtype='str')
        df_drive_final = pd.concat([df_drive, df_drive_hist])
        df_drive_final = df_drive_final.reset_index(drop=True)
        df_drive_final.to_csv('./production_tables/drive.csv', index=False)

    def create_series_table(self, df_play_by_play):
        df_series = df_play_by_play.filter(['game_id', 'drive_id', 'series_id', 'series', 'series_success',
                                            'series_result'])
        df_series = df_series.rename(columns={'series_id': 'id'})
        df_series = df_series.drop_duplicates()
        df_series = df_series.reset_index(drop=True)

        df_series_hist = pd.read_csv(self.series_hist_file, dtype='str')
        df_series_final = pd.concat([df_series, df_series_hist])
        df_series_final = df_series_final.reset_index(drop=True)
        df_series_final.to_csv('./production_tables/series.csv', index=False)

    def create_play_staging_df(self, df_play_by_play):
        df_play = df_play_by_play.drop(columns=['series_success', 'series_result', 'fixed_drive',
                                                'fixed_drive_result', 'drive_real_start_time', 'drive_play_count',
                                                'drive_time_of_possession', 'drive_first_downs', 'drive_inside20',
                                                'drive_ended_with_score', 'drive_quarter_start', 'drive_quarter_end',
                                                'drive_yards_penalized', 'drive_start_transition',
                                                'drive_end_transition',
                                                'drive_game_clock_start', 'drive_game_clock_end',
                                                'drive_start_yard_line',
                                                'drive_play_id_started', 'drive_play_id_ended', 'home_team',
                                                'away_team', 'season_type', 'game_date', 'stadium', 'location',
                                                'result', 'total', 'spread_line', 'total_line', 'div_game', 'roof',
                                                'surface',
                                                'home_coach', 'away_coach', 'stadium_id', 'game_stadium', 'away_score',
                                                'home_score'])

        df_play_hist = pd.read_csv(self.play_hist_file, dtype='str')
        df_play_final = pd.concat([df_play, df_play_hist])
        df_play_final = df_play_final.reset_index(drop=True)
        df_play_final.to_csv('./production_tables/play.csv', index=False)
        return df_play

    def create_play_player_roles_staging_df(self, df_play):
        # Add player accomplishments
        # Scoring points
        points_pp = ['td', 'kicker']
        # TODO what do I do with two_point_conversion_result

        df_play_player = pd.DataFrame(
            columns=['uuid', 'game_id', 'season_week_id', 'player_id', 'role', 'role_type', 'points', 'yards',
                     'penalty_type'])

        df_play['kicker_points'] = np.where(df_play['field_goal_result'] == 'made', 3, 0) + np.where(
            df_play['extra_point_result'] == 'good', 1, 0)
        df_play.replace({'kicker_points': {0: np.nan}}, inplace=True)

        for n in points_pp:
            p_id = n + '_player_id'
            df_temp = df_play.dropna(subset=[p_id])
            df_role = df_temp.filter(['uuid', 'game_id', 'season_week_id', p_id, 'kicker_points'])
            df_role['role'] = n
            df_role = df_role.rename(columns={p_id: 'player_id'})
            if n == 'td':
                df_role['points'] = 6
            else:
                df_role['points'] = df_role['kicker_points']
            df_role['role_type'] = 'points'
            df_role = df_role.filter(
                ['uuid', 'game_id', 'season_week_id', 'player_id', 'role', 'role_type', 'points', 'yards',
                 'penalty_type'])
            df_play_player = pd.concat([df_play_player, df_role])

        # Tackling etc.
        regular_pp = ['lateral_sack', 'interception', 'lateral_interception',
                      'lateral_punt_returner', 'lateral_kickoff_returner', 'punter', 'own_kickoff_recovery', 'blocked',
                      'tackle_for_loss_1', 'tackle_for_loss_2', 'qb_hit_1', 'qb_hit_2', 'forced_fumble_player_1',
                      'forced_fumble_player_2', 'solo_tackle_1', 'solo_tackle_2', 'assist_tackle_1', 'assist_tackle_2',
                      'assist_tackle_3', 'assist_tackle_4', 'tackle_with_assist_1', 'tackle_with_assist_2', 'fumbled_1',
                      'fumbled_2', 'fumble_recovery_1', 'fumble_recovery_2', 'sack', 'half_sack_1', 'half_sack_2',
                      'safety']

        for n in regular_pp:
            p_id = n + '_player_id'
            df_temp = df_play.dropna(subset=[p_id])
            df_role = df_temp.filter(['uuid', 'game_id', 'season_week_id', p_id])
            df_role['role'] = n
            df_role['role_type'] = 'other'
            df_role = df_role.rename(columns={p_id: 'player_id'})
            df_role = df_role.filter(
                ['uuid', 'game_id', 'season_week_id', 'player_id', 'role', 'role_type'])
            df_play_player = pd.concat([df_play_player, df_role])

        # Yards
        yards_pp = ['passer', 'receiver', 'rusher', 'lateral_receiver', 'lateral_rusher', 'penalty', 'punt_returner',
                    'kickoff_returner', 'penalty']

        for n in yards_pp:
            p_id = n + '_player_id'
            df_temp = df_play.dropna(subset=[p_id])
            df_role = df_temp.filter(
                ['uuid', 'game_id', 'season_week_id', p_id, 'passing_yards', 'receiving_yards', 'rushing_yards',
                 'lateral_receiving_yards', 'lateral_rushing_yards',
                 'penalty_type', 'penalty_yards', 'return_yards'])
            df_role['role'] = n
            df_role = df_role.rename(columns={p_id: 'player_id'})
            if n == 'passer':
                df_role['yards'] = df_role['passing_yards']
                df_role['penalty_type'] = ''
            elif n == 'receiver':
                df_role['yards'] = df_role['receiving_yards']
                df_role['penalty_type'] = ''
            elif n == 'rusher':
                df_role['yards'] = df_role['rushing_yards']
                df_role['penalty_type'] = ''
            elif n == 'lateral_receiver':
                df_role['yards'] = df_role['lateral_receiving_yards']
                df_role['penalty_type'] = ''
            elif n == 'lateral_rusher':
                df_role['yards'] = df_role['lateral_rushing_yards']
                df_role['penalty_type'] = ''
            elif n == 'punt_returner' or n == 'kickoff_returner':
                df_role['yards'] = df_role['return_yards']
                df_role['penalty_type'] = ''
            elif n == 'penalty':
                df_role['yards'] = df_role['penalty_yards']
            df_role['role_type'] = 'yards'
            df_role = df_role.filter(
                ['uuid', 'game_id', 'season_week_id', 'player_id', 'role', 'role_type', 'points', 'yards',
                 'penalty_type'])
            df_role = df_role.dropna()
            df_play_player = pd.concat([df_play_player, df_role])

        df_play_player.penalty_type.replace('', pd.NA)

        # Add play_player_uuid to the accomplishments table
        df_play_player = df_play_player.rename(columns={'uuid': 'play_uuid'})
        df_play_player['uuid'] = df_play_player[['play_uuid', 'player_id']].agg('_'.join, axis=1)
        df_play_player['last_update'] = self.today
        df_play_player['season_week_player_id'] = df_play_player[['season_week_id', 'player_id']].agg(
            '_'.join, axis=1)
        df_play_player = df_play_player.filter(
            ['last_update', 'uuid', 'game_id', 'season_week_player_id', 'player_id', 'role', 'role_type',
             'points', 'yards', 'penalty_type'])
        df_play_player = df_play_player.reset_index(drop=True)

        df_play_player_hist = pd.read_csv(self.play_player_hist_file, dtype='str')
        df_play_player_final = pd.concat([df_play_player, df_play_player_hist])
        df_play_player_final = df_play_player_final.reset_index(drop=True)
        df_play_player_final.to_csv('./production_tables/play_player.csv', index=False)
        return df_play_player
