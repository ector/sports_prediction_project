import time
import pandas as pd
from multiprocessing import Pool
from pymongo import MongoClient

from tools.games_feed.data_manipulation import ExtractAndManipulateData
from tools.games_feed.matches import Matches
from tools.utils import get_config
from te_logger.logger import log

mongodb_uri = get_config("db").get("sport_prediction_url")


class SaveFootballApiData(object):

    def __init__(self):
        # MyLogger.__init__(self)
        self.football_data = None
        self.filename = 'full.csv'
        self.league_code = ''
        self.data_directory = 'prototype/data/raw_data/{}.csv'
        # self.log = log

    def download_football_api_data(self):
        """
        :rtype: object
        """
        pieces = []
        clmns = ["Comp_id", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]

        self.matches = Matches()
        self.emd = ExtractAndManipulateData()
        self.matches.save_matches(comp_id=self.league_code)
        matches_list = self.emd.matches_extract_and_manipulate_by_id(comp_id=self.league_code)

        df = pd.DataFrame(matches_list)

        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)
        self.football_data = df
        self.merge_to_existing_data()

    def merge_to_existing_data(self):
        """Merge data if any exist"""
        client = MongoClient(mongodb_uri, connectTimeoutMS=30000)

        db = client.get_database("sports_prediction")
        wdw_raw_data = db.wdw_raw_data

        self.football_data = self.football_data.dropna(how='any')

        for idx, ft_data in self.football_data.iterrows():
            ft_data = dict(ft_data)
            exist = {'Date': ft_data.get('Date'), 'HomeTeam': ft_data.get('HomeTeam'), 'AwayTeam': ft_data.get('AwayTeam'),
                     'Comp_id': ft_data.get('Comp_id')}
            wdw_count = wdw_raw_data.find(exist).count()

            if int(wdw_count) == 0:
                print("inserting {0}".format(ft_data))
                wdw_raw_data.insert_one(ft_data)

            ## Only use this when adding new field/attribute to the data
            # elif int(wdw_count) == 1:
            #     log.info("updating {0}".format(ft_data))
            #     wdw_raw_data.update_one(exist, , {'$set': ft_data})

    def download_league_data(self, league):
        """
        download leagues data
        :return: 
        """
        self.filename = league
        league_data = get_config(file="football_api_com")
        self.league_code = league_data.get(league)
        log.info("{}: {}".format(league, self.league_code))
        self.download_football_api_data()


if __name__ == '__main__':
    sfad = SaveFootballApiData()
    leagues_data = get_config(file="football_api_com")
    league_list = list(leagues_data.keys())
    print(league_list)
    p = Pool(processes=10)
    p.map(sfad.download_league_data, league_list)
