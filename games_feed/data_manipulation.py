# -*- coding: utf-8 -*-
"""
Created on 29-01-2018 at 12:19 PM 

@author: desco
"""


import json
from datetime import datetime


def ft_score(fthg, ftag):
    fthg = int(fthg)
    ftag = int(ftag)
    if fthg > ftag:
        return 'H'
    elif fthg < ftag:
        return 'A'
    else:
        return 'D'

def season_calc(season):
    season1 = season[2:4]
    season2 = season[-2:]
    new_season = season1 + season2
    return new_season

def date_change(old_format="05.11.2017"):
    datetimeobject = datetime.strptime(old_format, '%d.%m.%Y')
    new_format = datetimeobject.strftime('%Y-%m-%d')
    return new_format


json_file = open('www.json', "r")
data = json.load(json_file)

clubs = []

for i in data:
    footy = {
            'Season': season_calc(season=i['season']),
            'Date': date_change(old_format=i['formatted_date']),
            'Week': i['week'],
            'HomeTeam': i['localteam_name'],
            'AwayTeam': i['visitorteam_name'],
            'FTHG': i['localteam_score'],
            'FTAG': i['visitorteam_score'],
            'FTR': ft_score(fthg=i['localteam_score'], ftag=i['visitorteam_score']),
            'Div': i["comp_id"]
            }
    clubs.append(i["localteam_name"])
    clubs.append(i["visitorteam_name"])
    print(footy)

print(list(set(clubs)))


