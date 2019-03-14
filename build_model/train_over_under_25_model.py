# -*- coding: utf-8 -*-
"""
Created on 31-12-2017 at 5:10 PM 

@author: tola
"""
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib

from utils import get_analysis_root_path, get_config
from te_logger.logger import log

leagues_data = get_config(file="league")
model_columns = get_config(file="model_columns")
leagues = list(leagues_data.keys())


for league in leagues:
    try:
        games = pd.read_csv(get_analysis_root_path('tools/data/clean_data/team_trend/{}.csv'.format(league)))
        games = games.dropna(how='any')

        model_columns = get_config(file="ou25_columns/{}".format(league))
        data = games.loc[(games.Season.isin([1415, 1516, 1617, 1718, 1819])) & (games.played == 1)]

        target = data.UO25.values

        # Data without target
        data = data[model_columns]

        model = LogisticRegression(C=1e5)
        model.fit(data, target)
        log.info("League: {}\t score: {}".format(league, model.score(data, target)))

        model_filename = get_analysis_root_path("tools/league_models/{}_ou25".format(league))
        joblib.dump(model, model_filename)
    except Exception as e:
        log.warn("New O/U 2.5 model not built for {} \n{}".format(league.upper(), e))

log.info("Finished training over under 2.5 model")
