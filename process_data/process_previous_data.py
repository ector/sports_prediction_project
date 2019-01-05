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


def split_current_trend(trend, k):
    value_list = list(trend.get(k))
    while len(value_list) != 5:
        value_list.append(None)
    return value_list


class ProcessPreviousData(object):
    def __init__(self):
        self.clean_team_trend_data_directory = get_analysis_root_path('tools/data/clean_data/team_trend/{}.csv')

    def compute_trend(self, data, lg):
        """
        Compute the trend for each team
        """
        data.loc[:, "HTREND"] = ""
        data.loc[:, "HTREND2"] = ""
        data.loc[:, "HTREND3"] = ""
        data.loc[:, "HTREND4"] = ""
        data.loc[:, "HTREND5"] = ""
        data.loc[:, "ATREND"] = ""
        data.loc[:, "ATREND2"] = ""
        data.loc[:, "ATREND3"] = ""
        data.loc[:, "ATREND4"] = ""
        data.loc[:, "ATREND5"] = ""
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
            # home_trend = string_to_array(''.join(team_home_data.FTR))

            # team away trend
            team_away_data = data[(data["AwayTeam"] == team_num)]
            team_away_data.loc[:, "FTR"] = team_away_data.FTR.map({"H": "L", "D": "D", "A": "W"}).values
            # away_trend = string_to_array(''.join(team_away_data.FTR))

            # team Overall trend
            team_overall_data = pd.concat([team_home_data, team_away_data])
            team_overall_data = team_overall_data.sort_index()
            overall_trend = {idx: val for idx, val in
                             zip(team_overall_data.index, string_to_array(''.join(team_overall_data.FTR)))}

            # Set the trend for home 5 home trend
            i = 0
            for k, tdt in team_home_data.iterrows():
                data.loc[k, ["HTREND", "HTREND2", "HTREND3", "HTREND4", "HTREND5"]] = split_current_trend(overall_trend, k=k)
                i += 1

            # Set the trend for away 5 away team
            i = 0
            for k, tdt in team_away_data.iterrows():
                data.loc[k, ["ATREND", "ATREND2", "ATREND3", "ATREND4", "ATREND5"]] = split_current_trend(overall_trend, k=k)
                i += 1

        # Replace empty string with D
        data.HTREND.replace("", "D", inplace=True)
        data.ATREND.replace("", "D", inplace=True)
        trend = get_config('ldw')
        data.loc[:, "HTREND"] = data.HTREND.map(trend).values
        data.loc[:, "HTREND2"] = data.HTREND2.map(trend).values + data.HTREND.values
        data.loc[:, "HTREND3"] = data.HTREND3.map(trend).values + data.HTREND2.values
        data.loc[:, "HTREND4"] = data.HTREND4.map(trend).values + data.HTREND3.values
        data.loc[:, "HTREND5"] = data.HTREND5.map(trend).values + data.HTREND4.values
        data.loc[:, "ATREND"] = data.ATREND.map(trend).values
        data.loc[:, "ATREND2"] = data.ATREND2.map(trend).values + data.ATREND.values
        data.loc[:, "ATREND3"] = data.ATREND3.map(trend).values + data.ATREND2.values
        data.loc[:, "ATREND4"] = data.ATREND4.map(trend).values + data.ATREND3.values
        data.loc[:, "ATREND5"] = data.ATREND5.map(trend).values + data.ATREND4.values
        data.loc[:, "HPOINT"] = data.groupby(["Season", "HomeTeam"])["HTREND"].cumsum()
        data.loc[:, "APOINT"] = data.groupby(["Season", "AwayTeam"])["ATREND"].cumsum()

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

        for s in range(1, 6):
            data.loc[:, "HLM_{}".format(s)] = data.groupby("HomeTeam")["HLM"].shift(s)
            data.loc[:, "ALM_{}".format(s)] = data.groupby("AwayTeam")["ALM"].shift(s)
            data.loc[:, "AAG_{}".format(s)] = data.groupby(["AwayTeam"])["FTAG"].shift(s)
            data.loc[:, "HAG_{}".format(s)] = data.groupby(["HomeTeam"])["FTHG"].shift(s)
            data.loc[:, "AAGC_{}".format(s)] = data.groupby(["AwayTeam"])["FTHG"].shift(s)
            data.loc[:, "HAGC_{}".format(s)] = data.groupby(["AwayTeam"])["FTAG"].shift(s)

        data.loc[:, "ACUM"] = data.groupby(["Season", "AwayTeam"])["ALM_1"].cumsum()
        data.loc[:, "HCUM"] = data.groupby(["Season", "HomeTeam"])["HLM_1"].cumsum()
        data = data.drop(["A", "D", "H"], axis=1).dropna()

        data.loc[:, "AAG"] = data.groupby(["Season", "AwayTeam"])["AAG_1"].apply(
            lambda x: x.rolling(window=5, min_periods=1).mean())
        data.loc[:, "HAG"] = data.groupby(["Season", "HomeTeam"])["HAG_1"].apply(
            lambda x: x.rolling(window=5, min_periods=1).mean())

        data.loc[:, "AAGC"] = data.groupby(["Season", "AwayTeam"])["AAGC_1"].apply(
            lambda x: x.rolling(min_periods=1, window=5).mean())
        data.loc[:, "HAGC"] = data.groupby(["Season", "HomeTeam"])["HAGC_1"].apply(
            lambda x: x.rolling(min_periods=1, window=5).mean())

        data = data.dropna()
        return data

    def store_significant_columns(self, lg="england_premiership"):
        self.log = log

        self.log.info("Processing {} data".format(lg))

        fix_path = get_analysis_root_path('tools/data/fixtures/selected_fixtures/{}.csv'.format(lg))
        if os.path.exists(fix_path):
            fix_data = pd.read_csv(fix_path)
            fix_data.loc[:, "FTHG"] = 0
            fix_data.loc[:, "FTAG"] = 0
            fix_data.loc[:, "FTR"] = 'D'
            fix_data.loc[:, "Season"] = 1819
            fix_data.loc[:, "played"] = 0
            fix_data.loc[:, "BTTS"] = 0

            client = MongoClient(mongodb_uri, connectTimeoutMS=30000)
            db = client.get_database("sports_prediction")
            lg_data = db[lg]

            data = pd.DataFrame(list(lg_data.find({})), columns=["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR",
                                                             "Season"])
            data = team_translation(data=data, league=lg)
            data.loc[:, "played"] = 1
            data.loc[:, "FTHG"] = pd.to_numeric(data.FTHG.values)
            data.loc[:, "FTAG"] = pd.to_numeric(data.FTAG.values)
            data.loc[(data.FTHG > 0) & (data.FTAG > 0), 'BTTS'] = 1
            data.loc[(data.FTHG == 0) | (data.FTAG == 0), 'BTTS'] = 0

            agg_data = pd.concat([data, fix_data], ignore_index=True, sort=False)

            agg_data = self.compute_last_point_ave_goals_and_goals_conceded(data=agg_data, lg=lg)
            agg_data = agg_data.fillna(0)
            agg_data.loc[:, 'UO25'] = list(np.where((pd.to_numeric(agg_data.FTHG.values) + pd.to_numeric(agg_data.FTAG.values)) > 2.5, 1, 0))

            #TODO: make sure that played_data is used to find significant columns
            played_data = agg_data[agg_data["played"] == 1]

            target_real = played_data.FTR.map({"A": -3, "D": 0, "H": 3})
            dc_real = played_data.FTR.map({"A": 1, "D": 0, "H": 0})
            ou25_target = played_data.UO25
            played_data = played_data.drop(['FTR', 'FTHG', 'FTAG', 'UO25', "HLM", "ALM", 'BTTS'], axis=1)

            wdw_coef_data = played_data.corrwith(target_real)
            wdw_sig_cols = list(played_data.drop(["Date", "played", "Time"], axis=1).columns)
            wdw_sig_data = wdw_coef_data.where(wdw_coef_data.abs() > 0.05)
            wdw_sig_data = wdw_sig_data.dropna()
            if len(list(wdw_sig_data.index)) != 0:
                wdw_sig_cols = list(wdw_sig_data.index)
            save_league_model_attr(model="wdw_columns", league=lg, cols=wdw_sig_cols)

            dc_coef_data = played_data.corrwith(dc_real)
            dc_sig_cols = list(played_data.drop(["Date", "played", "Time"], axis=1).columns)
            dc_sig_data = dc_coef_data.where(dc_coef_data.abs() > 0.05)
            dc_sig_data = dc_sig_data.dropna()
            if len(list(dc_sig_data.index)) != 0:
                dc_sig_cols = list(dc_sig_data.index)
            save_league_model_attr(model="dc_columns", league=lg, cols=dc_sig_cols)

            ou25_coef_data = played_data.corrwith(ou25_target)
            ou25_sig_cols = list(played_data.drop(["Date", "played", "Time"], axis=1).columns)
            ou25_sig_data = ou25_coef_data.where(ou25_coef_data.abs() > 0.05)
            ou25_sig_data = ou25_sig_data.dropna()
            if len(list(ou25_sig_data.index)) != 0:
                ou25_sig_cols = list(ou25_sig_data.index)

            save_league_model_attr(model="ou25_columns", league=lg, cols=ou25_sig_cols)

            # date without time
            agg_data["Date"] = [pd.to_datetime(str(d)).date() for d in agg_data.Date.values]

            agg_data = agg_data.drop(['FTHG', 'FTAG'], axis=1)
            agg_data.to_csv(self.clean_team_trend_data_directory.format(lg), index=False)
            self.log.info("{} data saved in clean folder".format(lg.upper()))
        else:
            self.log.warning("{} not processed as there are no fixtures for the next 3 days".format(lg).upper())


if __name__ == '__main__':
    ppd = ProcessPreviousData()
    leagues_data = get_config(file="leagues_id")
    league_list = list(leagues_data.keys())
    # for league in league_list:
    #     ppd.store_significant_columns(lg=league)
    p = Pool(processes=20)
    p.map(ProcessPreviousData().store_significant_columns, league_list)
