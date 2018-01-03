import time
import pandas as pd
from multiprocessing import Pool
from pymongo import MongoClient
from tools.utils import get_config

mongodb_uri = get_config("db").get("sport_prediction_url")


class PullData(object):

    def __init__(self):
        # MyLogger.__init__(self)
        self.football_data = None
        self.filename = 'full.csv'
        self.league_code = ''
        self.data_directory = 'prototype/data/raw_data/{}.csv'

    def download_football_data(self):
        """
        :rtype: object
        """
        pieces = []
        clmns = ["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR", "HC", "AC", "HS",
                 "AS", "HST", "AST"]
        for i in range(17, 18):
            try:
                year = str(i).zfill(2) + str(i + 1).zfill(2)
                print("Year {}".format(year), self.league_code)
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
        client = MongoClient(mongodb_uri, connectTimeoutMS=30000)

        db = client.get_database("sports_prediction")
        wdw_raw_data = db.wdw_raw_data

        self.football_data = self.football_data.dropna(how='any')

        for idx, ft_data in self.football_data.iterrows():
            ft_data = dict(ft_data)
            exist = {'Date': ft_data.get('Date'), 'HomeTeam': ft_data.get('HomeTeam'), 'AwayTeam': ft_data.get('AwayTeam'),
                     'Div': ft_data.get('Div')}
            wdw_count = wdw_raw_data.find(exist).count()

            if int(wdw_count) == 0:
                print("inserting {}".format(ft_data))
                wdw_raw_data.insert_one(ft_data)

            elif int(wdw_count) == 1:
                print("updating {}".format(ft_data))
                wdw_raw_data.update(exist, ft_data)

    def download_league_data(self, league):
        """
        download leagues data
        :return: 
        """
        self.filename = league
        league_data = get_config(file="leagues_id")
        self.league_code = league_data.get(league)
        print(league, self.league_code)
        self.download_football_data()


if __name__ == '__main__':
    dr = PullData()
    leagues_data = get_config(file="leagues_id")
    league_list = list(leagues_data.keys())
    p = Pool(processes=10)
    p.map(dr.download_league_data, league_list)
