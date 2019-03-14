# -*- coding: utf-8 -*-
"""
Created on 13-09-2017 at 8:54 PM

@author: tola
"""
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
from imblearn.over_sampling import SMOTE

from utils import get_analysis_root_path, get_config
from te_logger.logger import log


leagues_data = get_config(file="league")
leagues = list(leagues_data.keys())


for league in leagues:
    log.info(msg="Building model for league: {}".format(league))
    lg_data_path = get_analysis_root_path('tools/data/clean_data/team_trend/{}.csv'.format(league))
    try:
        games = pd.read_csv(lg_data_path)
        games = games.dropna(how='any')

        dc_columns = get_config(file="dc_columns/{}".format(league))
        played_data = games.loc[(games.Season.isin([1415, 1516, 1617, 1718, 1819])) & (games.played == 1)]

        target_1x = played_data.FTR.map({"D": 0, "A": 1, "H": 0})

        # Select significant columns
        dc_data = played_data[dc_columns]

        # Double chance model fit
        sm = SMOTE(random_state=2)
        dc_data_res, target_1x_res = sm.fit_sample(dc_data, target_1x.ravel())

        model = LogisticRegression(C=1e5)
        model.fit(dc_data_res, target_1x_res)
        log.info("0: '1x', 1: 'A' League: {}\t DC score: {}".format(league, model.score(dc_data_res, target_1x_res)))
        model_filename = get_analysis_root_path("tools/league_models/{}_dc".format(league))
        joblib.dump(model, model_filename)

    except Exception as e:
        log.warn("New wdw model not built for {}".format(league).upper())
        log.warn("See why:::::: {}".format(e))
log.info("Finished wdw training model")
