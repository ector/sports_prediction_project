# -*- coding: utf-8 -*-
"""
Created on 13-09-2017 at 8:54 PM 

@author: tola
"""
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib

from utils import get_analysis_root_path, get_config
from te_logger.logger import log


leagues_data = get_config(file="league")
model_columns = get_config(file="model_columns")
leagues = list(leagues_data.keys())


for league in leagues:
    log.info(msg="Building model for league: {}".format(league))
    lg_data_path = get_analysis_root_path('tools/data/clean_data/team_trend/{}.csv'.format(league))
    try:
        games = pd.read_csv(lg_data_path)
        games = games.dropna(how='any')

        model_columns = get_config(file="wdw_columns/{}".format(league)).get(league)
        dc_columns = get_config(file="dc_columns/{}".format(league)).get(league)
        played_data = games.loc[(games.Season.isin([1415, 1516, 1617, 1718, 1819])) & (games.played == 1)]

        target = played_data.FTR.map({"D": 0, "A": 1, "H": 2})
        target_1x = played_data.FTR.map({"D": 0, "A": 1, "H": 0})
        log.info("{} significant columns: {}".format(league.upper(), model_columns))

        # Select significant columns
        data = played_data[model_columns]
        dc_data = played_data[dc_columns]

        model = LogisticRegression(C=1e5)
        model.fit(data, target)
        log.info("League: {}\t score: {}".format(league, model.score(data, target)))
        model_filename = get_analysis_root_path("tools/league_models/{}".format(league))
        joblib.dump(model, model_filename)

        # Double chance model fit
        model = LogisticRegression(C=1e5)
        model.fit(dc_data, target_1x)
        log.info("0: '1x', 1: 'A' League: {}\t DC score: {}".format(league, model.score(dc_data, target_1x)))
        model_filename = get_analysis_root_path("tools/league_models/{}_1x".format(league))
        joblib.dump(model, model_filename)

    except Exception as e:
        log.warn("New wdw model not built for {}".format(league).upper())
        log.warn("See why:::::: {}".format(e))
log.info("Finished wdw training model")
