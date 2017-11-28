# -*- coding: utf-8 -*-
"""
Created on 13-09-2017 at 8:54 PM 

@author: tola
"""

import json
import pandas as pd
from sklearn.preprocessing import StandardScaler

from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.externals import joblib
from tools.utils import get_analysis_root_path, get_config

leagues_data = get_config(file="leagues_id")
leagues = list(leagues_data.keys())

for league in leagues:
    games = pd.read_csv(get_analysis_root_path('prototype/data/clean_data/team_trend/{}.csv'.format(league)))
    games = games.dropna(how='any')
    games = games.set_index(['Date'])
    data = games.loc[games.Season.isin([1516, 1617, 1718])]
    data = data.drop(['Season', 'FTHG', 'FTAG'], axis=1)
    data = data.sample(frac=1)
    target = data.FTR.map({'D': 1, 'A': 2, 'H': 3})
    # Gent without target
    data = data.drop('FTR', axis=1)
    data = pd.get_dummies(data)

    # print(ft.get(league))
    data_cols = list(data.columns)
    print(data_cols)
    data_cols_filename = get_analysis_root_path("prototype/league_models/{}_cols".format(league))
    joblib.dump(data_cols, data_cols_filename)

    stdsc = StandardScaler()
    data_std = stdsc.fit_transform(data)
    stdsc_filename = get_analysis_root_path("prototype/league_models/{}_stdsc".format(league))
    joblib.dump(data, stdsc_filename)

    # model = LogisticRegression(C=1e5, max_iter=3000)
    model = SVC(kernel='rbf', C=1.0, gamma=0.1, random_state=12, probability=True)
    model.fit(data_std, target)
    print("League: {}\t score: {}".format(league, model.score(data_std, target)))

    model_filename = get_analysis_root_path("prototype/league_models/{}".format(league))
    joblib.dump(model, model_filename)

print("Finished training model")
