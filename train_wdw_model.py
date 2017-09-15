# -*- coding: utf-8 -*-
"""
Created on 13-09-2017 at 8:54 PM 

@author: tola
"""

import json
import pandas as pd
from sklearn.preprocessing import StandardScaler

from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
from tools.utils import get_analysis_root_path

leagues = ["england_premiership", "england_championship", "england_league1", "england_league2", "scotland_premiership",
      "scotland_championship", "scotland_league1", "scotland_league2", "germany_bundesliga", "germany_bundesliga2",
      "france_le_championnat", "france_division_2", "netherlands_eredivisie", "belgium_jupiler", "portugal_liga_1",
      "italy_serie_a", "italy_serie_b", "spain_la_liga_premera", "spain_la_liga_segunda", "turkey_futbol_ligi_1"]

for league in leagues:
    games = pd.read_csv(get_analysis_root_path('prototype/data/clean_data/{}'.format(league)))
    games = games.dropna(how='any')
    games = games.set_index(['Date'])
    data = games.loc[games.Season.isin([1415, 1516, 1617, 1718])]
    data = data.drop(['Season'], axis=1)

    target = data.FTR.map({'D': 1, 'A': 2, 'H': 3})
    # Gent without target
    data = data.drop('FTR', axis=1)
    data = pd.get_dummies(data)

    pass_rate = get_analysis_root_path("tools/config/pass_rate.json")
    with open(pass_rate, "r") as readFile:
        ft = json.load(readFile)

    # print(ft.get(league))
    ft.get(league)['attr'] = list(data.columns)

    with open(pass_rate, "w") as writeFile:
        json.dump(ft, writeFile)

    model = LogisticRegression(C=1e5)

    stdsc = StandardScaler()
    data_std = stdsc.fit_transform(data)
    stdsc_filename = get_analysis_root_path("prototype/league_models/{}_stdsc".format(league))
    joblib.dump(data_std, stdsc_filename)

    model.fit(data_std, target)

    model_filename = get_analysis_root_path("prototype/league_models/{}".format(league))
    joblib.dump(model, model_filename)

print("Finished training model")
