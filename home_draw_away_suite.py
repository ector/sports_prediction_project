import os

import datetime
import pandas as pd
import numpy as np
from pandas import DataFrame
from sklearn.preprocessing import LabelEncoder

from te_logger.logger import MyLogger


# noinspection PyCallByClass
class DeriveFootballFeatures(MyLogger):
    """
    Use this class to derive features needed for football 
    """
    def __init__(self):
        self.class_le = LabelEncoder()
        self.inverse_result = None
        MyLogger.logger(self)

    def home_and_away_team_mapper(self, data, mapper, info="original"):
        """
        encodes and decodes HomeTeam and AwayTeam columns of the dataFrame
        :param data: dataFrame with home and away team  
        :param mapper: dict mapper
        :param info: string
        :return: dataFrame 
        """
        try:
            data['HomeTeam'] = data.HomeTeam.map(mapper)
            self.log.info("{} HomeTeam data mapped".format(info.capitalize()))
        except:
            pass

        try:
            data['AwayTeam'] = data.AwayTeam.map(mapper)
            self.log.info("{} AwayTeam data mapped".format(info.capitalize()))
        except:
            pass

        return data

    def encode_teams(self, data):
        """
        attach a unique id to each team
        :param data: dataFrame with HomeTeam and AwayTeam
        :return: keys and values 
        """
        # Team mapping
        self.log.info("Starts encoding HomeTeam and AwayTeam data mapped")
        team_mapping = {label: idx for idx, label in
                        enumerate(np.unique(data.HomeTeam.tolist() + data.AwayTeam.tolist()))}
        self.log.info("Completed HomeTeam and AwayTeam data mapping")
        return team_mapping

    def decode_teams(self, team_mapping):
        """
        Reverse keys and values
        :param team_mapping: dict
        :return: keys and values 
        """
        # Team mapping
        self.log.info("Starts decoding team data to inverse")
        team_mapping = {team_mapping.get(key): key for key in team_mapping.keys()}
        self.log.info("Completed decoding team data")
        return team_mapping

    def get_list_teams(self, data):
        """
        :param data: dataFrame with HomeTeam and AwayTeam
        :return: array list
        """
        self.log.info("Getting the names of teams")
        teams = np.unique(data.HomeTeam.tolist() + data.AwayTeam.tolist())
        self.log.info("Got the names of teams: {}".format(teams))
        return teams
