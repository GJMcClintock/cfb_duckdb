#%%
import dlt
import requests
from settings import API_KEY
import sys
import json

url = 'https://api.collegefootballdata.com/'
headers = {
    'Authorization': API_KEY
}

from datetime import datetime

backfill = len(sys.argv) > 1 and sys.argv[1].lower() == "backfill"


def generate_seasons(start_year=1869, backfill=False):
    current_date = datetime.now()
    current_year = current_date.year

    # If it's January, use the previous year
    if current_date.month == 1:
        end_year = current_year - 1
    else:
        end_year = current_year

    if backfill:
        return list(range(start_year, end_year + 1))
    else:
        return [end_year]

# Example usage
seasons = generate_seasons(backfill=backfill)
secondary_seasons = generate_seasons(start_year=2002,backfill=backfill)

@dlt.resource(write_disposition='merge',primary_key=('season','week','season_type'))
def calendar():
    for season in seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team_id'))
def records():
    for season in seasons:
        endpoint_url = url + 'records'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = r.pop('year')
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','player_id','category','stat_type'))
def player_season_stats():
    for season in seasons:
        endpoint_url = url + 'stats/player/season'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = season
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team','stat_name'))
def season_stats():
    for season in seasons:
        endpoint_url = url + 'stats/season'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def season_stats_advanced():
    for season in seasons:
        endpoint_url = url + 'stats/season/advanced'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def ratings_sp():
    for season in seasons:
        endpoint_url = url + 'ratings/sp'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = r.pop('year')
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def ratings_srs():
    for season in seasons:
        endpoint_url = url + 'ratings/srs'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = r.pop('year')
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def ratings_elo():
    for season in seasons:
        endpoint_url = url + 'ratings/elo'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = r.pop('year')
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def ratings_fpi():
    for season in seasons:
        endpoint_url = url + 'ratings/fpi'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = r.pop('year')
        yield response


@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def ppa_teams():
    for season in seasons:
        endpoint_url = url + 'ppa/teams'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','id'))
def ppa_player_season():
    for season in seasons:
        endpoint_url = url + 'ppa/players/season'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','id'))
def recruiting_players():
    for season in seasons:
        endpoint_url = url + 'recruiting/players'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = r.pop('year')
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def recruiting_teams():
    for season in seasons:
        endpoint_url = url + 'recruiting/teams'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = r.pop('year')
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def fbs_teams():
    for season in seasons:
        endpoint_url = url + 'teams/fbs'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = season
        yield response

@dlt.resource(write_disposition='merge',primary_key=('season','team'))
def roster():
    for season in seasons:
        endpoint_url = url + 'roster'
        params = {
            'year' : season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            r['season'] = season
        yield response

@dlt.resource(write_disposition='merge',primary_key=('id'))
def games():
    for season in seasons:
        for season_type in ['regular','postseason']:
            endpoint_url = url + 'games'
            params = {
                'year' : season,
                'seasonType': season_type
            }
            response = requests.get(endpoint_url,params=params,headers=headers).json()
            yield response

@dlt.resource(write_disposition='merge',primary_key=('id'))
def game_media():
    for season in seasons:
        for season_type in ['regular','postseason']:
            endpoint_url = url + 'games/media'
            params = {
                'year' : season,
                'seasonType': season_type
            }
            response = requests.get(endpoint_url,params=params,headers=headers).json()
            yield response


@dlt.resource(write_disposition='merge',primary_key=('id'))
def drives():
    for season in seasons:
        for season_type in ['regular','postseason']:
            endpoint_url = url + 'drives'
            params = {
                'year' : season,
                'seasonType': season_type
            }
            response = requests.get(endpoint_url,params=params,headers=headers).json()
            yield response

@dlt.resource(write_disposition='merge',primary_key=('id'))
def game_player_stats():
    for season in seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year': season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            endpoint_url = url + 'games/players'
            params = {
                'year' : season,
                'seasonType': r.get('season_type'),
                'week': r.get('week')
            }
            game = requests.get(endpoint_url,params=params,headers=headers).json()
            yield game

@dlt.resource(write_disposition='merge',primary_key=('id'))
def game_team_stats():
    for season in seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year': season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            endpoint_url = url + 'games/teams'
            params = {
                'year' : season,
                'seasonType': r.get('season_type'),
                'week': r.get('week')
            }
            game = requests.get(endpoint_url,params=params,headers=headers).json()
            yield game

@dlt.resource(write_disposition='merge',primary_key=('id'))
def lines():
    for season in secondary_seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year': season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            endpoint_url = url + 'lines'
            params = {
                'year' : season,
                'seasonType': r.get('season_type'),
                'week': r.get('week')
            }
            game = requests.get(endpoint_url,params=params,headers=headers).json()
            yield game

@dlt.resource(write_disposition='merge',primary_key=('id','team'))
def ppa_game_team():
    for season in secondary_seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year': season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            endpoint_url = url + 'ppa/games'
            params = {
                'year' : season,
                'seasonType': r.get('season_type'),
                'week': r.get('week')
            }
            game = requests.get(endpoint_url,params=params,headers=headers).json()
            yield game

@dlt.resource(write_disposition='merge',primary_key=('season','name','team','week','season_type'))
def ppa_game_player():
    for season in secondary_seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year': season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            endpoint_url = url + 'ppa/players/games'
            params = {
                'year' : season,
                'seasonType': r.get('season_type'),
                'week': r.get('week')
            }
            game = requests.get(endpoint_url,params=params,headers=headers).json()
            if len(g) > 1:
                for g in game:
                    g['season_type'] = r.get('season_type')
                yield game

@dlt.resource(write_disposition='merge',primary_key=('id','team'))
def game_team_stats_advanced():
    for season in secondary_seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year': season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            endpoint_url = url + 'stats/game/advanced'
            params = {
                'year' : season,
                'seasonType': r.get('season_type'),
                'week': r.get('week')
            }
            game = requests.get(endpoint_url,params=params,headers=headers).json()
            yield game

@dlt.resource(write_disposition='merge',primary_key=('id'))
def plays():
    for season in secondary_seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year': season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            endpoint_url = url + 'plays'
            params = {
                'year' : season,
                'seasonType': r.get('season_type'),
                'week': r.get('week')
            }
            game = requests.get(endpoint_url,params=params,headers=headers).json()
            yield game

@dlt.resource(write_disposition='merge',primary_key=('season','week','season_type'))
def rankings():
    for season in seasons:
        endpoint_url = url + 'calendar'
        params = {
            'year': season
        }
        response = requests.get(endpoint_url,params=params,headers=headers).json()
        for r in response:
            endpoint_url = url + 'rankings'
            params = {
                'year' : season,
                'seasonType': r.get('season_type'),
                'week': r.get('week')
            }
            game = requests.get(endpoint_url,params=params,headers=headers).json()
            yield game

# %%

@dlt.source
def cfbd():
    return[calendar,recruiting_teams,recruiting_players,ppa_player_season,
           ppa_teams,ratings_elo,ratings_srs, ratings_sp,season_stats_advanced,season_stats,
           player_season_stats, records,fbs_teams,roster,games,game_media,
           ratings_fpi,drives,lines,plays,rankings,ppa_game_team,ppa_game_player,
           game_player_stats, game_team_stats, game_team_stats_advanced
           ]

    
pipeline = dlt.pipeline(
    pipeline_name='cfbd',
    destination='duckdb',
    dataset_name='ncaafb'
)

load_info = pipeline.run(cfbd())
print(load_info)