import pandas as pd
import fnmatch
import datetime as dt


def create_keys(df_play_by_play):
    df_play_by_play.drive = df_play_by_play.drive.fillna('')
    df_play_by_play['drive_id'] = df_play_by_play[['game_id', 'drive']].agg('_'.join, axis=1)
    df_play_by_play['series_id'] = df_play_by_play[['drive_id', 'series']].agg('_'.join, axis=1)
    df_play_by_play['play_uuid'] = df_play_by_play[['game_id', 'play_id']].agg('_'.join, axis=1)
    df_play_by_play['week2'] = df_play_by_play['week'].str.zfill(2)
    df_play_by_play['season_week_id'] = df_play_by_play[['season', 'week2']].agg('_'.join, axis=1)


class TransformPbP:

    def __init__(self, new_pbp_file, hist_pbp_file, today, last_season):
        self.new_pbp_file = new_pbp_file
        self.hist_pbp_file = hist_pbp_file
        self.today = today
        self.last_season = last_season

    def prep_new_pbp(self):
        df_pbp = pd.read_csv(self.new_pbp_file, dtype='str')
        # Create a list of the columns I want
        non_player_pbp_columns_used = ['game_id', 'drive', 'series', 'play_id', 'drive_id', 'old_game_id', 'home_team',
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
        create_keys(df_play_by_play)
        return df_play_by_play

