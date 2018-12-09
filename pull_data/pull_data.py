import time
import pandas as pd
from multiprocessing.pool import Pool

import requests
from pymongo import MongoClient

from utils import get_config
from te_logger.logger import log

mongodb_uri = get_config("db").get("sport_prediction_url")
translation = get_config("team_translation")


class PullData(object):

    def __init__(self):
        self.football_data = None
        self.filename = None
        self.league_code = ''

    def download_football_data(self):
        """
        :rtype: object
        """
        pieces = []
        clmns = ["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]

        data_url = 'http://www.football-data.co.uk/mmz4281/{year}/{league_id}.csv'

        for i in range(18, 19):
            year = str(i).zfill(2) + str(i + 1).zfill(2)
            formated_data_url = data_url.format(year=year, league_id=self.league_code)
            log.info("Year: {0}, League code: {1}, URL: {2}".format(year, self.league_code, formated_data_url))

            if requests.get(formated_data_url).status_code == 200:

                dd = pd.read_csv(formated_data_url, error_bad_lines=False, usecols=clmns)

                dd['Date'] = pd.to_datetime(dd['Date'], dayfirst=True)
                dd = dd[clmns]
                dd['Season'] = year
                dd["Comp_id"] = dd["Div"]
                dd = dd.drop('Div', axis=1)
                pieces.append(dd)
                time.sleep(2)
            try:
                data = pd.concat(pieces, ignore_index=True)
                data = data.drop_duplicates(subset=['Date', 'HomeTeam', 'AwayTeam', 'Season'], inplace=False)
                self.football_data = data.copy()
                self.merge_to_existing_data()
            except ValueError:
                pass
        return self

    def merge_to_existing_data(self):
        """Merge data if any exist"""
        client = MongoClient(mongodb_uri, connectTimeoutMS=30000)

        db = client.get_database("sports_prediction")
        wdw_raw_data = db[self.filename]

        self.football_data = self.football_data.dropna(how='any')

        translate = translation.get(self.filename)
        if translate:
            self.football_data["HomeTeam"].replace(translate, inplace=True)
            self.football_data["AwayTeam"].replace(translate, inplace=True)

        for idx, ft_data in self.football_data.iterrows():
            ft_data = dict(ft_data)
            exist = {'Date': ft_data.get('Date'), 'HomeTeam': ft_data.get('HomeTeam'), 'AwayTeam': ft_data.get('AwayTeam'),
                     'Comp_id': ft_data.get('Comp_id')}
            wdw_count = wdw_raw_data.find(exist).count()

            if int(wdw_count) == 0:
                log.info("inserting {0}".format(ft_data))
                wdw_raw_data.insert_one(ft_data)

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
    p = Pool(processes=15)
    p.map(dr.download_league_data, league_list)
