import json
import requests
import boto3

def get_roster(team):
    response = requests.get("https://data.nba.net/v2015/json/mobile_teams/nba/2021/teams/%s_roster.json" % team.lower())
    roster = json.loads(response.content)
    players = roster['t']['pl']
    team_name = {'firstname':roster['t']['tc'], 'lastname':roster['t']['tn']}
    name_players = [players,team_name]
    return name_players

def put_players(player,team,client):
    playername = player['fn'] + " " + player['ln']
    teamname = team['firstname'] + " " + team['lastname']
    #key = player['fn'] + "_" + player['ln'] + "_" + team['firstname'] + "_" +team['lastname']
    num = player['num']
    pos = player['pos']
    ht = player['ht']
    table_name = 'Roster'
    item={
        #'key' : {'S':key},
        'playername' : {'S':playername},
        'teamname' : {'S' : teamname},
        'num': {'S': num},
        'pos': {'S': pos},
        'ht': {'S': ht}
    }

    response = client.put_item(TableName = table_name, Item = item)
    return response


if __name__ == '__main__':
    #blazer missing due to API errors
    team_list = ['Hawks' , 'Celtics', 'Nets','Hornets', 'Bulls', 'Cavaliers',
                 'Mavericks', 'Nuggets', 'Pistons','Warriors', 'Rockets', 'Pacers',
                 'Clippers', 'Lakers', 'Grizzlies','Heat', 'Bucks', 'Timberwolves',
                 'Pelicans', 'Knicks', 'Thunder','Magic', '76ers', 'Suns',
                 'Kings', 'Spurs', 'Raptors', 'Jazz', 'Wizards']
    all_teams = []
    for team in team_list:
        all_teams.append(get_roster(team))

    client = boto3.client(
        'dynamodb',
        aws_access_key_id='AKIAVWFDYEZFMZAQKNJX',
        aws_secret_access_key='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',
    )
    resource = boto3.resource(
        'dynamodb',
        aws_access_key_id='AKIAVWFDYEZFMZAQKNJX',
        aws_secret_access_key='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',
    )
    for team in all_teams:
        players = team[0]
        team = team[1]
        for player in players:
            response = put_players(player,team,client)
            print(response)


