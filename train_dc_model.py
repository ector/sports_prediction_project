# -*- coding: utf-8 -*-
"""
Created on 13-09-2017 at 8:54 PM 

@author: tola
"""

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib

from tools.utils import get_analysis_root_path, get_config
from te_logger.logger import log


leagues_data = get_config(file="league")
model_columns = get_config(file="model_columns")
leagues = list(leagues_data.keys())

for league in leagues:
    log.info(msg="Building double change model for league: {}".format(league))
    games = pd.read_csv(get_analysis_root_path('prototype/data/clean_data/team_trend/{}.csv'.format(league)))
    games = games.dropna(how='any')
    # games = games.set_index(['Date'])
    data = games.loc[games.Season.isin([1417, 1516, 1617, 1718])]
    data = data[model_columns.get("match_result_cols")]
    data = data.sample(frac=1)
    target = data.FTR.map({'D': 0, 'A': 1, 'H': 0})
    # Gent without target
    data = data.drop(['Date', 'FTR', 'Season', 'FTHG', 'FTAG'], axis=1)
    # data = data.drop('FTR', axis=1)
    data = pd.get_dummies(data)

    # print(ft.get(league))
    data_cols = list(data.columns)
    log.info("length of data column {}".format(len(data_cols)))
    data_cols_filename = get_analysis_root_path("prototype/league_models/{}_dc_cols".format(league))
    joblib.dump(data_cols, data_cols_filename)

    stdsc = StandardScaler()
    data_std = stdsc.fit_transform(data)
    stdsc_filename = get_analysis_root_path("prototype/league_models/{}_dc_stdsc".format(league))
    joblib.dump(data, stdsc_filename)

    model = LogisticRegression(C=1e5)
    model.fit(data_std, target)
    log.info("League: {}\t score: {}".format(league, model.score(data_std, target)))

    model_filename = get_analysis_root_path("prototype/league_models/{}_dc".format(league))
    joblib.dump(model, model_filename)

log.info("Finished training double chance model")
