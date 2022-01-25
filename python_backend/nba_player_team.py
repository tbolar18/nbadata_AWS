from nba_api.stats.endpoints import shotchartdetail
import json
import pandas as pd
import requests


team_list = [['Atlanta Hawks',100],['Boston Celtics',101],['Brooklyn Nets',116],
             ['Charlotte Hornets',825],['Chicago Bulls',103],['Cleveland Cavaliers',104],
             ['Dallas Mavericks',105],['Denver Nuggets',106],['Detroit Pistons',107],
             ['Golden State Warriors',108],['Houston Rockets',109],['Indiana Pacers',110],
             ['Los Angeles Clippers',111],['Los Angeles Lakers',112],['Memphis Grizzlies',127],
             ['Miami Heat',113],['Milwaukee Bucks',114],['Minnesota Timberwolves',115],
             ['New Orleans Pelicans',102],['New York Knicks',117],['Oklahoma City Thunder',1827],
             ['Orlando Magic',118],['Philadelphia 76ers',119],['Phoenix Suns',120],
             ['Portland Trail Blazers',121],['Sacramento Kings',122],['San Antonio Spurs',123],
             ['Toronto Raptors',125],['Utah Jazz',126],['Washington Wizards',128]]

all_teams = []

for team in team_list:
    num_team = team[1]
    current_df = pd.read_html('https://www.proballers.com/basketball/team/%d' % num_team)
    current_df = current_df[0]
    current_df['Team'] = team[0]
    all_teams.append(current_df)

all_teams_dfs = pd.concat(all_teams)
player_team_df = all_teams_dfs[['Player','Pos.','Height','Age','Team']]
player_team_df.to_excel('output.xlsx')

