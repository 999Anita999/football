import pandas as pd
import fnmatch
import datetime as dt


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

    def __init__(self, new_pbp_file, hist_pbp_file, today, last_season):
        self.new_pbp_file = new_pbp_file
        self.hist_pbp_file = hist_pbp_file
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

    @staticmethod
    def create_game_table(df_play_by_play):
        # Filter down to the columns I need and clean it up
        df_game = df_play_by_play.filter(
            ['last_update', 'game_id', 'season_week_id', 'home_team', 'away_team', 'season_type', 'week',
             'game_date', 'season', 'stadium', 'location', 'result', 'total', 'spread_line',
             'total_line', 'div_game', 'roof', 'surface', 'home_coach', 'away_coach', 'stadium_id',
             'game_stadium', 'away_score', 'home_score'])
        df_game = df_game.drop_duplicates()
        df_game = df_game.rename(columns={'game_id':'id'})

        # Use the 'result' column to create a column saying if it was a home team win
        df_game['result'] = df_game['result'].astype('float')
        df_game['home_win'] = df_game.apply(convert_result, axis=1)

        df_game.to_csv('./production_tables/game.csv', index=False)
        return df_game

    @staticmethod
    def create_game_team_table(df_game):
        # Creating a table that has a record for every game for every team with infor for that team
        df_game= df_game.rename(columns={'id':'game_id'})

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
        # Since 'point_differential' is for the home team (originally 'result), flip it for the away team
        df_away['point_differential'] = df_away['result'] * -1
        df_away = df_away.drop(['result', 'home_win'], axis=1)
        df_away['home_away'] = 'away'

        # Combine the two into one df
        df_team = pd.concat([df_home, df_away])
        df_team = df_team.reset_index(drop=True)

        # Add season_week_team_id which will be the uuid on this table and a season_team foreign key for team data
        df_team['id'] = df_team[['season_week_id', 'team']].agg('_'.join, axis=1)
        df_team['season_team_id'] = df_team[['season', 'team']].agg('_'.join, axis=1)

        df_team.to_csv('./production_tables/game_team.csv', index=False)

    @staticmethod
    def create_drive_table(df_play_by_play):
        df_drive = df_play_by_play.filter(['game_id', 'drive_id', 'drive', 'posteam', 'posteam_type', 'defteam', 'fixed_drive',
                              'fixed_drive_result', 'drive_real_start_time', 'drive_play_count',
                              'drive_time_of_possession', 'drive_first_downs', 'drive_inside20',
                              'drive_ended_with_score', 'drive_quarter_start', 'drive_quarter_end',
                              'drive_yards_penalized', 'drive_start_transition', 'drive_end_transition',
                              'drive_game_clock_start', 'drive_game_clock_end', 'drive_start_yard_line',
                              'drive_play_id_started', 'drive_play_id_ended'])
        df_drive = df_drive.rename(columns={'drive_id':'id'})
        df_drive = df_drive.drop_duplicates()
        df_drive = df_drive.reset_index(drop=True)

        df_drive.to_csv('./production_tables/drive.csv', index=False)

    @staticmethod
    def create_series_table(df_play_by_play):
        df_series = df_play_by_play.filter(['game_id', 'drive_id', 'series_id', 'series', 'series_success',
                                            'series_result'])
        df_series = df_series.rename(columns={'series_id': 'id'})
        df_series = df_series.drop_duplicates()
        df_series = df_series.reset_index(drop=True)

        df_series.to_csv('./production_tables/series.csv', index=False)



