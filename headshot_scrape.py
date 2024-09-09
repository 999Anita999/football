from bs4 import BeautifulSoup
import requests
import pandas as pd

# Get list of teams
team_response = requests.get("https://www.nfl.com/standings/")
team_page = team_response.text
team_soup = BeautifulSoup(team_page, "html.parser")
club_name_raw = [n.get('href') for n in team_soup.select(".d3-o-club-info")]
clean_name1 = [sub.replace('/teams/', '') for sub in club_name_raw]
clean_club_name = [sub.replace('/', '') for sub in clean_name1]

# Get players and links
df_player_link = pd.DataFrame(columns=['full_name','club_name','picture'])

for club in clean_club_name:
    # Get the code from the team's roster site
    team_url = f"https://www.nfl.com/teams/{club}/roster"
    response = requests.get(team_url)
    roster_page = response.text
    soup = BeautifulSoup(roster_page, "html.parser")

    # Get the team's elements with the name and url
    element = soup.select(".d3-o-media-object")

    raw_headshots = []

    # Grab the raw code that has both the name and URL for each player and add to raw_headshots list
    for n in element:
        name = n.get_text()
        picture_code = n.select_one('img')
        record = f"{name}|{picture_code}"
        raw_headshots.append(record)

    # Turn the list into a dataframe, clean it up and add it to the dataframe
    df = pd.DataFrame({'raw_code': raw_headshots})
    df = df.replace(r'\n', ' ', regex=True)
    df['picture'] = [x.split('|')[-1] for x in df['raw_code']]
    df['picture'] = df['picture'].replace('<img alt="" class="img-responsive" src="', '', regex=True)
    df['picture'] = df['picture'].replace('"/>', '', regex=True)
    df['picture'] = df['picture'].replace('/t_lazy', '', regex=True)
    df['full_name'] = [x.split(' |')[0] for x in df['raw_code']]
    df['full_name'] = df['full_name'].str.strip()
    df = df.filter(['full_name', 'picture'])
    df['club_name'] = club
    df_player_link = pd.concat([df_player_link, df])

# Get unique list of players and merge in the urls
df_roster = pd.read_csv('./production_tables/roster.csv', dtype='str')
df_roster = df_roster.filter(['full_name','gsis_id'])
df_roster = df_roster.drop_duplicates()
df_player_headshots = df_roster.merge(df_player_link, how='left', on='full_name')

df_team_logo = pd.read_csv('./staging_tables/images/team_sm_logo.csv')
df_player_headshots = df_player_headshots.merge(df_team_logo, how='left', on='club_name')

# If there is no picture for them, use their team logo
def replace_missing_pics(df):
    if df['picture'] == 'None':
        return df['team_logo']
    else:
        return df['picture']


df_player_headshots['picture'] = df_player_headshots.apply(replace_missing_pics, axis=1)

# If they aren't on any rosters, use the NFL logo
df_player_headshots['picture'] = df_player_headshots['picture'].apply(
    lambda x: 'https://static.www.nfl.com/image/upload/v1554321393/league/nvfr7ogywskqrfaiu38m.svg' if pd.isnull(
        x) else x)

df_player_headshots.to_csv('./production_tables/player_headshot.csv', index=False)
