import os

import pandas as pd
import time
# from te_logger.logger import MyLogger


# class PullData(MyLogger):
class PullData(object):

    def __init__(self):
        # MyLogger.__init__(self)
        self.football_data = None
        self.filename = 'full.csv'
        self.league_code = ''
        self.data_directory = 'data/raw_data/'
        self.over_under_file = 'over_under.csv'

    def download_football_data(self):
        """
        :rtype: object
        """
        pieces = []
        for i in range(17, 18):
            try:
                year = str(i).zfill(2) + str(i + 1).zfill(2)
                print("Year {}".format(year))
                data = 'http://football-data.co.uk/mmz4281/' + year + '/' + self.league_code + '.csv'
                dd = pd.read_csv(data, error_bad_lines=False, parse_dates=['Date'])
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
            existing_data = pd.read_csv(self.data_directory + self.filename)
            frames = [existing_data, self.football_data]
            df = pd.DataFrame(pd.concat(frames))
        except IOError:
            df = self.football_data

        self.football_data = df.drop_duplicates()
        self.football_data.to_csv(self.data_directory + self.filename, index=False)

    def download_league_data(self):
        """
        download leagues data
        :return: 
        """
        leagues = {
                   'england_premiership': 'E0',
                   'england_championship': 'E1',
                   'england_league1': 'E2',
                   'england_league2': 'E3',

                   'scotland_premiership': 'SC0',
                   'scotland_championship': 'SC1',
                   'scotland_league1': 'SC2',
                   'scotland_league2': 'SC3',

                   'germany_bundesliga': 'D1',
                   'germany_bundesliga2': 'D2',

                   'italy_serie_a': 'I1',
                   'italy_serie_b': 'I2',

                   'spain_la_liga_premera': 'SP1',
                   'spain_la_liga_segunda': 'SP2',

                   'france_le_championnat': 'F1',
                   'france_division_2': 'F2',

                   'netherlands_eredivisie': 'N1',

                   'belgium_jupiler': 'B1',

                   'portugal_liga_1': 'P1',

                   'turkey_futbol_ligi_1': 'T1',

                   'greece_ethniki_katigoria': 'G1',
                   }
        for league in leagues.keys():
            self.filename = league
            self.league_code = leagues.get(league)
            print(league)
            self.download_football_data()


if __name__ == '__main__':
    dr = PullData()
    dr.download_league_data()
