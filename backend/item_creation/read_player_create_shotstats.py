from nba_api.stats.endpoints import shotchartdetail
import json
from json.decoder import JSONDecodeError
import requests
import boto3
from boto3.dynamodb.conditions import Key
import datetime
from dateutil.relativedelta import relativedelta
error_players = []

def pop_list_values(indices_to_delete,list):
    indices = sorted(indices_to_delete, reverse=True)
    for index in indices:
        if index < len(list):
            list.pop(index)
    return list

def get_team_id(q_team):
  for team in teams:
    if team['teamName'] == q_team:
      return team['teamId']
  return -1

def get_player_id(first, last):
  for player in players:
    if player['firstName'] == first and player['lastName'] == last:
      return player['playerId']
  return -1

def putshotdetail(shot_data_rows,shot_data_headers,resource):
    shot_data_headers_intial = shot_data_headers.copy()
    indices_to_delete = [4, 6]
    shot_data_headers_final = pop_list_values(indices_to_delete,shot_data_headers_intial)

    table= resource.Table('ShotsDetails')
    with table.batch_writer() as batch:
        index = 0
        for row in shot_data_rows:
            key_list = [index, row[1],row[3], row[7], row[8], row[9],row[21]]
            key = '-'.join([str(i) for i in key_list])
            print(key)
            playername = row[4]
            teamname = row[6]

            indices_to_delete = [4, 6]
            row_final = pop_list_values(indices_to_delete,row)
            row_dict = dict(zip(shot_data_headers_final,row_final))
            index = index + 1
            batch.put_item(
                Item = {
                    'key': key,
                    'playername': playername,
                    'teamname': teamname,
                    'info': row_dict
                }
            )





def getshotdetail(current_player, current_team,resource):
    firstN, lastN = current_player.split(' ', 1)
    last_month_date = datetime.date.today() - relativedelta(months=1)
    fromdate = datetime.date.strftime(last_month_date, "%m/%d/%Y")
    try:
        shot_json = shotchartdetail.ShotChartDetail(
                    team_id = get_team_id(current_team),
                    player_id = get_player_id(firstN,lastN),
                    context_measure_simple = 'FGA',
                    season_nullable= '2021-22',
                    date_from_nullable= fromdate,
                    season_type_all_star = 'Regular Season')

        shot_data = json.loads(shot_json.get_json())
        shot_data_rows = shot_data['resultSets'][0]['rowSet']
        shot_data_headers = shot_data['resultSets'][0]['headers']
        putshotdetail(shot_data_rows,shot_data_headers,resource)

    except JSONDecodeError as e:
        error_player = firstN + " " + lastN + " " + current_team
        error_players.append(error_player)
    return error_players


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
        aws_access_key_id='AKIAVWFDYEZFMZAQKNJX',
        aws_secret_access_key='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',
    )
    resource = boto3.resource(
        'dynamodb',
        aws_access_key_id='AKIAVWFDYEZFMZAQKNJX',
        aws_secret_access_key='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',
    )
    teams = json.loads(requests.get('https://raw.githubusercontent.com/bttmly/nba/master/data/teams.json').text)
    players = json.loads(requests.get('https://raw.githubusercontent.com/bttmly/nba/master/data/players.json').text)
    data = query_for_players(client)

    for player in data:
        current_player = player['playername']['S']
        current_team = player['teamname']['S']
        error_players = getshotdetail(current_player,current_team,resource)
