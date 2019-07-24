import time
from multiprocessing.pool import Pool

import pandas as pd
import requests
from pymongo import MongoClient

from te_logger.logger import log
from utils import get_config

mongodb_uri = get_config("db").get("sport_prediction_url")
translation = get_config("team_translation")

start_yr = 19


class PullData(object):

    def __init__(self):
        # self.football_data = None
        self.filename = None
        self.league_code = ''

    def download_football_data(self):
        """
        :rtype: object
        """
        pieces = []
        # TODO: Remove totally to capture all data
        # clmns = ["Div","Date","HomeTeam","AwayTeam","FTHG","FTAG","FTR","HTHG","HTAG","HTR","HS","AS","HST","AST",
        # "HF","AF","HC","AC","HY","AY","HR","AR","B365H","B365D","B365A","BWH","BWD","BWA"]

        data_url = 'http://www.football-data.co.uk/mmz4281/{year}/{league_id}.csv'
        for i in range(start_yr, start_yr+1):
            year = str(i).zfill(2) + str(i + 1).zfill(2)
            formated_data_url = data_url.format(year=year, league_id=self.league_code)
            log.info("Year: {0}, League code: {1}, URL: {2}".format(year, self.league_code, formated_data_url))

            if requests.get(formated_data_url).status_code == 200:
                try:
                    dd = pd.read_csv(formated_data_url, encoding='utf-8', error_bad_lines=False)
                except:
                    dd = pd.read_csv("http://www.football-data.co.uk/mmz4281/1819/I2.csv", encoding='ISO-8859-1',
                                     error_bad_lines=False)

                dd['Date'] = pd.to_datetime(dd['Date'], dayfirst=True)
                dd['Season'] = year
                dd["Comp_id"] = dd["Div"]
                dd = dd.drop('Div', axis=1)
                pieces.append(dd)
                time.sleep(2)
        try:
            # data = pd.concat(pieces, ignore_index=True, sort=True)
            data = dd.drop_duplicates(subset=['Date', 'HomeTeam', 'AwayTeam', 'Season'], inplace=False)
            data.rename(columns={'BbAv<2.5': 'BbAv<25', 'BbAv>2.5': 'BbAv>25',
                                 'BbMx<2.5': 'BbMx<25', 'BbMx>2.5': 'BbMx>25'}, inplace=True)
            # print(data.columns)
            football_data = data.copy()
            # print(football_data)
            self.merge_with_existing_data(ft_data=football_data)
        except ValueError:
            pass

        return self

    def merge_with_existing_data(self, ft_data):
        """Merge data if any exist"""
        client = MongoClient(mongodb_uri, connectTimeoutMS=30000)

        db = client.get_database("sports_prediction")
        wdw_raw_data = db[self.filename]

        ft_data = ft_data.dropna(how='any')

        translate = translation.get(self.filename)
        if translate:
            ft_data["HomeTeam"].replace(translate, inplace=True)
            ft_data["AwayTeam"].replace(translate, inplace=True)

        try:
            dta = ft_data.to_dict("record")
            if dta:
                wdw_raw_data.insert_many(dta)
                print(self.filename, " Saved")
            else:
                print("I cant store".upper())
        except Exception as e:
            log.info("Encountered Error:{} \n League: {}".format(e, self.filename))

    def download_league_data(self, league):
        """
        download leagues data
        :return: 
        """
        self.filename = league
        league_data = get_config(file="leagues_id")
        self.league_code = league_data.get(league)
        log.info("{}: {}".format(league, self.league_code))
        self.download_football_data()


if __name__ == '__main__':
    dr = PullData()
    leagues_data = get_config(file="leagues_id")
    league_list = list(leagues_data.keys())
    p = Pool(processes=10)
    p.map(dr.download_league_data, league_list)
