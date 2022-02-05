from nba_api.stats.endpoints import shotchartdetail
import json
from json.decoder import JSONDecodeError
import requests
import boto3
import datetime
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

def putshotdetail(player,team,shot_dict,resource):

    table= resource.Table('ShotsDetails')

    with table.batch_writer() as batch:
        key_list = [player,team]
        key = '-'.join([str(i) for i in key_list])

        batch.put_item(
            Item = {
                'key': key,
                'info': shot_dict
            }
        )
        print(player)

def getshotdetail(current_player, current_team, players_list, teams_list, resource):

    firstN, lastN = current_player.split(' ', 1)


    try:
        shot_json = shotchartdetail.ShotChartDetail(
                    team_id = get_team_id(current_team, teams_list),
                    player_id = get_player_id(firstN,lastN, players_list),
                    context_measure_simple = 'FGA',
                    season_nullable= '2021-22',
                    date_from_nullable= '12/28/2021',
                    date_to_nullable= '01/28/2022',
                    season_type_all_star = 'Regular Season')

        shot_data = json.loads(shot_json.get_json())
        shot_data_rows = shot_data['resultSets'][0]['rowSet']
        shot_data_headers = shot_data['resultSets'][0]['headers']
        shot_row_dict = [dict(zip(shot_data_headers,row)) for row in shot_data_rows]
        putshotdetail(current_player,current_team,shot_row_dict, resource)

    except JSONDecodeError as e:
        error_player = firstN + " " + lastN + " " + current_team
        error_players.append(error_player)


def query_for_players(client):
    response = client.scan(
        TableName = 'Roster'
    )
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response  = client.scan(
            TableName = 'Roster',
            ExclusiveStartKey = response['LastEvaluatedKey']
        )
        data.extend(response['Items'])

    return data

if __name__ == '__main__':

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


    teams = json.loads(requests.get('https://raw.githubusercontent.com/bttmly/nba/master/data/teams.json').text)
    players = json.loads(requests.get('https://raw.githubusercontent.com/bttmly/nba/master/data/players.json').text)

    data = query_for_players(client)
    for player in data:
        current_player = player['playername']['S']
        current_team = player['teamname']['S']
        getshotdetail(current_player,current_team,players,teams, resource)
