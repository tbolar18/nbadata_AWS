from nba_api.stats.endpoints import shotchartdetail
import json
import pandas as pd
import requests

player_team_df = pd.read_excel('output.xlsx')

dict_player_team = player_team_df.drop(['Unnamed: 0','Pos.','Height','Age'], axis=1).to_dict(orient='records')
# Get team ID based on team name
teams = json.loads(requests.get('https://raw.githubusercontent.com/bttmly/nba/master/data/teams.json').text)
def get_team_id(q_team):
  for team in teams:
    if team['teamName'] == q_team:
      return team['teamId']
  return -1

# Get player ID based on player name
players = json.loads(requests.get('https://raw.githubusercontent.com/bttmly/nba/master/data/players.json').text)
def get_player_id(first, last):
  for player in players:
    if player['firstName'] == first and player['lastName'] == last:
      return player['playerId']
  return -1

all_dfs = []
for players_x in dict_player_team:
    team_name = players_x.get('Team')
    first_name, last_name = players_x.get('Player').split(' ',1)
    try:
        shot_json = shotchartdetail.ShotChartDetail(
                    team_id = get_team_id(team_name),
                    player_id = get_player_id(first_name,last_name),
                    context_measure_simple = 'PTS',
                    season_nullable= '2021-22',
                    season_type_all_star = 'Regular Season')

        shot_data = json.loads(shot_json.get_json())
        relevant_data = shot_data['resultSets'][0]
        headers = relevant_data['headers']
        rows = relevant_data['rowSet']

        data = pd.DataFrame(rows)
        data.columns = headers
        all_dfs.append(data)
    except:
        print(team_name + " " + first_name + " " + last_name)

all_team_dfs = pd.concat(all_dfs)
print(all_team_dfs)