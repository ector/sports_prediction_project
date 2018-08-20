# -*- coding: utf-8 -*-
"""
Created on 13-09-2017 at 8:54 PM 

@author: tola
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib

from utils import get_analysis_root_path, get_config, encode_data
from te_logger.logger import log


leagues_data = get_config(file="league")
model_columns = get_config(file="model_columns")
leagues = list(leagues_data.keys())


for league in leagues:
    log.info(msg="Building model for league: {}".format(league))
    games = pd.read_csv(get_analysis_root_path('prototype/data/clean_data/team_trend/{}.csv'.format(league)))
    games = games.dropna(how='any')

    model_columns = get_config(file="wdw_columns/{}".format(league)).get(league)
    played_data = games.loc[(games.Season.isin([1415, 1516, 1617, 1718, 1819])) & (games.played == 1)]

    # clubs = np.unique(data['AwayTeam'].tolist() + data['HomeTeam'].tolist())
    # team_mapping = {label: idx for idx, label in enumerate(clubs)}
    # stdsc_filename = get_analysis_root_path("prototype/league_models/{}_stdsc".format(league))
    # joblib.dump(team_mapping, stdsc_filename)

    # data_std = encode_data(data=data, team_mapping=team_mapping)
    # data_std = data_std.dropna(how="any")

    target = played_data.FTR
    target_1x = played_data.FTR.map({0: 0, -3: 1, 3: 0})
    target_x2 = played_data.FTR.map({0: 0, -3: 0, 3: 1})

    # Gent without target
    # data_std = data_std.drop(['Date', 'FTR', 'Season', 'FTHG', 'FTAG'], axis=1)
    # data_cols = list(data_std.columns)
    log.info("{} significant columns: {}".format(league.upper(), model_columns))

    # Select significant columns
    data = played_data[model_columns]

    model = LogisticRegression(C=1e5)
    model.fit(data, target)
    log.info("League: {}\t score: {}".format(league, model.score(data, target)))
    model_filename = get_analysis_root_path("prototype/league_models/{}".format(league))
    joblib.dump(model, model_filename)

    # Double chance model fit
    model = LogisticRegression(C=1e5)
    model.fit(data, target_1x)
    log.info("0: '1x', 1: 'A' League: {}\t DC score: {}".format(league, model.score(data, target_1x)))
    model_filename = get_analysis_root_path("prototype/league_models/{}_1x".format(league))
    joblib.dump(model, model_filename)

    model = LogisticRegression(C=1e5)
    model.fit(data, target_x2)
    log.info("0: 'x2', 1: 'H' League: {}\t DC score: {}".format(league, model.score(data, target_x2)))
    model_filename = get_analysis_root_path("prototype/league_models/{}_x2".format(league))
    joblib.dump(model, model_filename)

log.info("Finished wdw training model")
