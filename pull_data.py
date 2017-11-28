import os
import json
import pandas as pd
import time
from multiprocessing import Pool, Process
from functools import partial
from te_logger.logger import MyLogger


# class PullData(MyLogger):
from tools.utils import get_analysis_root_path, get_config


class PullData(object):

    def __init__(self):
        # MyLogger.__init__(self)
        self.football_data = None
        self.filename = 'full.csv'
        self.league_code = ''
        self.data_directory = 'prototype/data/raw_data/{}.csv'
        self.over_under_file = 'over_under.csv'

    def download_football_data(self):
        """
        :rtype: object
        """
        pieces = []
        clmns = ["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR"]
        for i in range(12, 18):
            try:
                year = str(i).zfill(2) + str(i + 1).zfill(2)
                print("Year {}".format(year))
                data = 'http://www.football-data.co.uk/mmz4281/' + year + '/' + self.league_code + '.csv'
                dd = pd.read_csv(data, error_bad_lines=False, usecols=clmns)
                dd['Date'] = pd.to_datetime(dd['Date'], dayfirst=True)
                dd = dd[clmns]
                dd['Season'] = year
                pieces.append(dd)
                time.sleep(2)
            except:
                pass
        try:
            self.football_data = pd.concat(pieces, ignore_index=True)
            self.merge_to_existing_data()
        except ValueError:
            pass
        return self

    def merge_to_existing_data(self):
        """Merge data if any exist"""
        try:
            existing_data = pd.read_csv(get_analysis_root_path(self.data_directory.format(self.filename)))
            frames = [existing_data, self.football_data]
            df = pd.DataFrame(pd.concat(frames))
        except IOError:
            df = self.football_data

        df = df.dropna(how='any')
        self.football_data = df.drop_duplicates(subset=["HomeTeam", "AwayTeam", "Season"], keep="last")
        self.football_data.to_csv(get_analysis_root_path(self.data_directory.format(self.filename)), index=False)

    def download_league_data(self, league):
        """
        download leagues data
        :return: 
        """
        self.filename = league
        leagues_data = get_config(file="leagues_id")
        self.league_code = leagues_data.get(league)
        print(league, self.league_code)
        self.download_football_data()


if __name__ == '__main__':
    dr = PullData()
    leagues_data = get_config(file="leagues_id")
    league_list = list(leagues_data.keys())
    p = Pool(processes=10)
    p.map(dr.download_league_data, league_list)
