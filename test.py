import pandas as pd
import datetime as dt

today = dt.date.today()

df = pd.read_csv('./rosters/roster_legacy.csv')
df['last_update'] = today

df = df.rename(columns={'gsis_id': 'player_id', 'season_week_player_id':'id'})

df = df.filter(
            ['last_update', 'id', 'season_team_id', 'player_id', 'season', 'week', 'team', 'full_name', 'position',
             'depth_chart_position', 'jersey_number', 'status', 'full_name', 'birth_date', 'height', 'weight',
             'college', 'entry_year', 'rookie_year', 'draft_club', 'draft_number']
        )

df.to_csv('./rosters/roster_weekly_hist.csv', index=False)