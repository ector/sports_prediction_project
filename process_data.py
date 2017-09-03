import os

import pandas as pd
from sklearn.preprocessing import LabelEncoder

from te_logger.logger import MyLogger
# from tools import DeriveFootballFeatures


# noinspection PyCallByClass
from tools.home_draw_away_suite import DeriveFootballFeatures


class ProcessData(MyLogger):
    def __init__(self):
        self.class_le = LabelEncoder()
        self.ftr_class = {'D': 1, 'A': 2, 'H': 3}
        self.inverse_ftr_class = None
        self.team_mapping = None
        self.football_data = GetFootballData()
        self.home_draw_away_suite = DeriveFootballFeatures()
        MyLogger.logger(self)

    def get_football_game_data(self, league="scotland_premiership", game={}):
        """
        Fetch football data w.r.t to the league and game
        :param league: string
        :param game: dict {'AwayTeam': john, 'HomeTeam': bristol}
        :return: dataFrame
        """
        data = self.football_data.get_football_data(league=league)

        # not sure this is necessary
        data = data[['AwayTeam', 'Date', 'FTR', 'HomeTeam', 'Season', 'HomeLastWin', 'AwayLastWin',
                    'HomeLast3Games', 'AwayLast3Games', 'HomeLast5Games', 'AwayLast5Games']]

        data = data.set_index('Date')
        self.log.info("Date set as index for {}".format(league))

        self.team_mapping = self.home_draw_away_suite.encode_teams(data=data)
        data = self.home_draw_away_suite.home_and_away_team_mapper(data=data, mapper=self.team_mapping)

        game_data = data.copy()
        print(game_data.tail())
        game_data = game_data.loc[game_data['Season'].isin([1617, 1718])]
        # self.log.info("Games Filtered {} - {}".format(league, game))

        game_data['FTR'] = game_data['FTR'].map(self.ftr_class)
        game_data = game_data.drop('Season', axis=1)
        self.inverse_ftr_class = {val: key for key, val in self.ftr_class.items()}

        print(game_data.tail())
        return game_data

    def get_team_names(self, league):
        """
        Get the names of the team in a league
        :param league: string - league name
        :return: unique list of teams
        """
        data = self.football_data.get_football_data(league=league)

        return self.home_draw_away_suite.get_list_teams(data=data)


class GetFootballData(MyLogger):
    def __init__(self):
        MyLogger.logger(self)

    def get_football_data(self, league):
        """
        Read the football league data from a file
        :return: dataframe
        """
        base_dir = "analysis"
        filename = "sports_betting/data/clean_data/{}".format(league)
        self.log.info("Starts reading file {}".format(filename))
        abs_path = os.path.abspath(filename).split(base_dir)[0]
        file_path = os.path.join(os.path.join(abs_path, base_dir), filename)
        data = pd.read_csv(file_path, parse_dates=['Date'])
        self.log.info("Finished reading: {}".format(file_path))
        return data


if __name__ == '__main__':
    dr = ProcessData()
    dr.get_football_game_data()