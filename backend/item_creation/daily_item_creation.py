from nba_api.stats.endpoints import shotchartdetail
import json
from json.decoder import JSONDecodeError
import requests
import boto3
from boto3.dynamodb.conditions import Key
from datetime import date
from dateutil.relativedelta import relativedelta


error_players = []

def pop_list_values(indices_to_delete,list):

    indices = sorted(indices_to_delete, reverse=True)

    for index in indices:
        if index < len(list):
            list.pop(index)

    return list

def get_team_id(q_team, teams):

  for team in teams:
    if team['teamName'] == q_team:
      return team['teamId']

  return -1

def get_player_id(first, last, players):

  for player in players:
    if player['firstName'] == first and player['lastName'] == last:
      return player['playerId']

  return -1

def getyesterdaygames():
    games_return = []
    yesterday_date = date.today() - relativedelta(days=1)
    start_date = date.strftime(yesterday_date,"%Y-%m-%d")
    response = requests.get("https://www.balldontlie.io/api/v1/games?&start_date=%s" % start_date)
    games_data = json.loads(response.content)
    games = games_data['data']
    for game in games:
        home_team = game['home_team']['full_name']
        visitor_team = game['visitor_team']['full_name']
        games_return.append(home_team)
        games_return.append(visitor_team)
    return games_return

def updateshotlist(outdated, new):
    latest_date_needed = date.today() - relativedelta(months = 1)
    latest_date_needed_formatted = latest_date_needed.strftime('%Y%m%d')
    try:
        removed_outdated_item = [x for x in outdated if x.get('GAME_DATE') >= latest_date_needed_formatted]
        removed_outdated_item.extend(new)
        return removed_outdated_item
    except TypeError:
        return outdated

def updateandputshotdetail(current_player,current_team,shot_row_dict, resource):
    key = current_player + "-" + current_team
    table= resource.Table('ShotsDetails')

    response = table.query(
        KeyConditionExpression=Key('key').eq(key)
    )
    outdated_info = response['Items'][0]['info']
    current_item = updateshotlist(outdated_info,shot_row_dict)

    table.update_item(
        Key = {
            'key': key
        },
        UpdateExpression = "set info = :new_info",
        ExpressionAttributeValues = {
            ':new_info' : current_item
        },
        ReturnValues = "UPDATED_NEW"

    )


def getshotdetail(firstN, lastN, current_team, players_list, teams_list, resource):

    last_date = date.today() - relativedelta(days=1)
    fromdate = date.strftime(last_date, "%m/%d/%Y")

    try:
        shot_json = shotchartdetail.ShotChartDetail(
                    team_id = get_team_id(current_team, teams_list),
                    player_id = get_player_id(firstN,lastN,players_list),
                    context_measure_simple = 'FGA',
                    season_nullable= '2021-22',
                    date_from_nullable= fromdate,
                    season_type_all_star = 'Regular Season')

        shot_data = json.loads(shot_json.get_json())
        shot_data_rows = shot_data['resultSets'][0]['rowSet']
        shot_data_headers = shot_data['resultSets'][0]['headers']
        shot_row_dict = [dict(zip(shot_data_headers, row)) for row in shot_data_rows]

        current_player = firstN + ' ' + lastN

        updateandputshotdetail(current_player,current_team,shot_row_dict, resource)

    except JSONDecodeError as e:
        error_player = firstN + " " + lastN + " " + current_team
        error_players.append(error_player)

def get_roster(team):
    response = requests.get("https://data.nba.net/v2015/json/mobile_teams/nba/2021/teams/%s_roster.json" % team.lower())
    roster = json.loads(response.content)

    players = roster['t']['pl']

    return players

#Main Function-Remotely Run by Task Scheduler
client = boto3.client(
    'dynamodb',
    aws_access_key_id='****',
    aws_secret_access_key='****',
)

resource = boto3.resource(
    'dynamodb',
    aws_access_key_id='****',
    aws_secret_access_key='****',
)

teams = list(set(getyesterdaygames()))

teams_list = json.loads(requests.get('https://raw.githubusercontent.com/bttmly/nba/master/data/teams.json').text)
players_list = json.loads(requests.get('https://raw.githubusercontent.com/bttmly/nba/master/data/players.json').text)

for team in teams:
    if team == 'Portland Trail Blazers':
        continue
    first, last = team.split(' ',1)
    if ' ' in last:
        last_2, last_1 = last.split(' ', 1)
        roster = get_roster(last_1)
    else:
        roster = get_roster(last)
    for player in roster:
        getshotdetail(player['fn'],player['ln'],team, players_list, teams_list, resource)


