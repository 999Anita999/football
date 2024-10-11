#pbp participation from main
else:
    # ------------process new participation file------------------
    new_participation_file = "./staging_tables/participation_by_play/pbp_participation_new.csv"
    play_player_no_role_hist_file = "./staging_tables/play_by_play/play_player_no_role_hist.csv"

    try:
        transform_pbp_participation = TransformPbPParticipation(new_participation_file, play_hist_file,
                                                                play_player_no_role_hist_file, play_player_hist_file,
                                                                today)
        df_play_plus = transform_pbp_participation.add_participation_to_play(df_play)
        transform_pbp_participation.add_participation_to_play_player(df_play_plus, df_play_player)

    except FileNotFoundError:
        print('No changes to pbp participation file, so none processed')

# pbp participation from retrieve_new_files, check timestamp
pbp_participation_timestamp_url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/timestamp.txt'
urllib.request.urlretrieve(pbp_participation_timestamp_url,
                           "./staging_tables/participation_by_play/pbp_participation_timestamp.csv")
pbp_participation_timestamp = "staging_tables/participation_by_play/pbp_participation_timestamp.csv"
last_nflverse_pbp_participation_update = convert_eastern_file_timestamp(pbp_participation_timestamp)

# pbp participation from retrieve_new_files, open
self.refresh_pbp_participation = last_refresh_timestamp < last_nflverse_pbp_participation_update

# pbp participation from retrieve_new_files, download new files
if self.refresh_pbp_participation:
    new_part_url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{self.this_season}.csv'
    try:
        urllib.request.urlretrieve(new_part_url,
                                   "./staging_tables/participation_by_play/pbp_participation_new.csv")
    except:
        print('No new pbp participation file')