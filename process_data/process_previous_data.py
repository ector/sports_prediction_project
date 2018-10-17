# -*- coding: utf-8 -*-
"""
Created on 15-06-2018 at 2:09 PM 

@author: tola
"""
import os

import numpy as np
import pandas as pd
from multiprocessing import Pool
from pymongo import MongoClient
from utils import get_config, save_league_model_attr, get_analysis_root_path, team_translation
from te_logger.logger import log


mongodb_uri = get_config("db").get("sport_prediction_url")
translation = get_config("team_translation")


def string_to_array(string, length=5):
    """
    Split string to a number of array
    """
    trend = []
    i = 0
    for _ in range(len(string)):
        tt = string[:i]
        trend.append(tt[-length:][::-1])
        i += 1
    return trend


class ProcessPreviousData(object):
    def __init__(self):
        self.clean_team_trend_data_directory = get_analysis_root_path('tools/data/clean_data/team_trend/{}.csv')

    def compute_trend(self, data, lg):
        """
        Compute the trend for each team
        """
        data["HTREND"] = ""
        data["ATREND"] = ""
        data["H5HTREND"] = ""
        data["A5ATREND"] = ""
        clubs = np.unique(data['AwayTeam'].tolist() + data['HomeTeam'].tolist())
        team_mapping = {label: idx for idx, label in enumerate(clubs)}
        save_league_model_attr(model="team_mapping", league=lg, cols=team_mapping)

        data.loc[:, "HomeTeam"] = data.HomeTeam.map(team_mapping)
        data.loc[:, "AwayTeam"] = data.AwayTeam.map(team_mapping)

        for team_num in team_mapping.values():
            # Store each game's outcome for the team

            # team home trend
            team_home_data = data[(data["HomeTeam"] == team_num)]
            team_home_data.loc[:, "FTR"] = team_home_data.FTR.map({"H": "W", "D": "D", "A": "L"}).values
            home_trend = string_to_array(''.join(team_home_data.FTR))

            # team away trend
            team_away_data = data[(data["AwayTeam"] == team_num)]
            team_away_data.loc[:, "FTR"] = team_away_data.FTR.map({"H": "L", "D": "D", "A": "W"}).values
            away_trend = string_to_array(''.join(team_away_data.FTR))

            # team Overall trend
            team_overall_data = pd.concat([team_home_data, team_away_data])
            team_overall_data = team_overall_data.sort_index()
            overall_trend = {idx: val for idx, val in
                             zip(team_overall_data.index, string_to_array(''.join(team_overall_data.FTR)))}

            # Set the trend for home 5 home trend
            i = 0
            for k, tdt in team_home_data.iterrows():
                data.loc[k, "H5HTREND"] = home_trend[i]
                data.loc[k, "HTREND"] = overall_trend.get(k)
                i += 1

            # Set the trend for away 5 away team
            i = 0
            for k, tdt in team_away_data.iterrows():
                data.loc[k, "A5ATREND"] = away_trend[i]
                data.loc[k, "ATREND"] = overall_trend.get(k)
                i += 1

        # Replace empty string with D
        data.H5HTREND.replace("", "D", inplace=True)
        data.A5ATREND.replace("", "D", inplace=True)
        data.HTREND.replace("", "D", inplace=True)
        data.ATREND.replace("", "D", inplace=True)
        trend = get_config('trend_code')
        data.loc[:, "HTREND"] = data.HTREND.map(trend).values
        data.loc[:, "ATREND"] = data.ATREND.map(trend).values
        data.loc[:, "H5HTREND"] = data.H5HTREND.map(trend).values
        data.loc[:, "A5ATREND"] = data.A5ATREND.map(trend).values

        #TODO: check if dropping na in trend makes the prediction better
        data = data.fillna(0)
        return data

    def compute_last_point_ave_goals_and_goals_conceded(self, data, lg):
        """
        Compute HLM, ALM, ACUM, HCUM, AAG, HAG, AAGC, HAGC
        :param lg:
        :param data:
        :return:
        """
        data = self.compute_trend(data, lg)

        data[["A", "D", "H"]] = pd.get_dummies(data.FTR)
        data.loc[:, "HLM"] = data.H * 3 + data.D
        data.loc[:, "ALM"] = data.A * 3 + data.D

        data.loc[:, "HLM"] = data.groupby("HomeTeam")["HLM"].shift(1).fillna(value=0)
        data.loc[:, "ALM"] = data.groupby("AwayTeam")["ALM"].shift(1).fillna(value=0)
        data.loc[:, "ACUM"] = data.groupby(["Season", "AwayTeam"])["ALM"].cumsum()
        data.loc[:, "HCUM"] = data.groupby(["Season", "HomeTeam"])["HLM"].cumsum()
        data = data.drop(["A", "D", "H"], axis=1).dropna()

        data.loc[:, "AAG"] = data.groupby(["AwayTeam"])["FTAG"].shift(1).fillna(0)
        data.loc[:, "AAG"] = data.groupby(["Season", "AwayTeam"])["AAG"].apply(
            lambda x: x.rolling(window=5, min_periods=1).mean())

        data.loc[:, "HAG"] = data.groupby(["HomeTeam"])["FTHG"].shift(1).fillna(0)
        data.loc[:, "HAG"] = data.groupby(["Season", "HomeTeam"])["HAG"].apply(
            lambda x: x.rolling(window=5, min_periods=1).mean())

        data.loc[:, "AAGC"] = data.groupby(["AwayTeam"])["FTHG"].shift(1).fillna(0)
        data.loc[:, "AAGC"] = data.groupby(["Season", "AwayTeam"])["AAGC"].apply(
            lambda x: x.rolling(min_periods=1, window=5).mean())

        data.loc[:, "HAGC"] = data.groupby(["AwayTeam"])["FTAG"].shift(1).fillna(0)
        data.loc[:, "HAGC"] = data.groupby(["Season", "HomeTeam"])["HAGC"].apply(
            lambda x: x.rolling(min_periods=1, window=5).mean())

        return data

    def store_significant_columns(self, lg="england_premiership"):
        self.log = log

        fix_path = get_analysis_root_path('tools/data/fixtures/selected_fixtures/{}.csv'.format(lg))
        if os.path.exists(fix_path):
            fix_data = pd.read_csv(fix_path)
            fix_data["FTHG"] = 0
            fix_data["FTAG"] = 0
            fix_data["FTR"] = 'D'
            fix_data["Season"] = 1819
            fix_data["played"] = 0

            client = MongoClient(mongodb_uri, connectTimeoutMS=30000)
            db = client.get_database("sports_prediction")
            lg_data = db[lg]

            data = pd.DataFrame(list(lg_data.find({})), columns=["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR",
                                                             "Season"])
            data = team_translation(data=data, league=lg)
            data["played"] = 1

            agg_data = pd.concat([data, fix_data], ignore_index=True, sort=False)

            agg_data = self.compute_last_point_ave_goals_and_goals_conceded(data=agg_data, lg=lg)


            #TODO: make sure that played_data is used to find significant columns
            played_data = agg_data[agg_data["played"] == 1]

            target_real = played_data.FTR.map({"A": -3, "D": 0, "H": 3})
            played_data.loc[:, 'ou25_target'] = list(np.where((played_data.HAG + played_data.AAG) > 2.5, 1, 0))
            ou25_target = played_data.ou25_target
            played_data = played_data.drop(['FTR', 'FTHG', 'FTAG', 'ou25_target'], axis=1)

            wdw_coef_data = played_data.corrwith(target_real)
            wdw_sig_data = wdw_coef_data.where(wdw_coef_data.abs() > 0.05)
            wdw_sig_data = wdw_sig_data.dropna()
            wdw_sig_cols = list(wdw_sig_data.index)
            save_league_model_attr(model="wdw_columns", league=lg, cols=wdw_sig_cols)

            ou25_coef_data = played_data.corrwith(ou25_target)
            ou25_sig_data = ou25_coef_data.where(ou25_coef_data.abs() > 0.05)
            ou25_sig_data = ou25_sig_data.dropna()
            ou25_sig_cols = list(ou25_sig_data.index)
            save_league_model_attr(model="ou25_columns", league=lg, cols=ou25_sig_cols)

            agg_data = agg_data.drop(['FTHG', 'FTAG'], axis=1)
            # agg_data["FTR"] = target_real
            agg_data.to_csv(self.clean_team_trend_data_directory.format(lg), index=False)
            self.log.info("{} data saved in clean folder".format(lg.upper()))
        else:
            self.log.warning("{} not processed as there are no fixtures for the next 3 days".format(lg).upper())


if __name__ == '__main__':
    ppd = ProcessPreviousData()
    leagues_data = get_config(file="leagues_id")
    league_list = list(leagues_data.keys())
    for league in league_list:
        ppd.store_significant_columns(lg=league)
    # p = Pool(processes=20)
    # p.map(ProcessPreviousData().store_significant_columns, league_list)
