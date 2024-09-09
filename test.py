import pandas as pd
import datetime as dt

df_2019 = pd.read_csv('./production_tables/play_player_2019.csv', dtype='str')
df_2020 = pd.read_csv('./production_tables/play_player_2020.csv', dtype='str')
df_2021 = pd.read_csv('./production_tables/play_player_2021.csv', dtype='str')
df_2022 = pd.read_csv('./production_tables/play_player_2022.csv', dtype='str')
df_2023 = pd.read_csv('./production_tables/play_player_2023.csv', dtype='str')
df_full = pd.concat([df_2019, df_2020, df_2021, df_2022, df_2023])
df_full = df_full.reset_index(drop=True)

df_full.to_csv('./staging_tables/play_by_play/play_player_hist.csv', index=False)

