import json
from difflib import get_close_matches

import numpy as np
import pandas as pd

from te_logger.logger import log
from tools.utils import get_analysis_root_path, get_start_and_end_dates, get_config, save_fixtures_to_file


class GameFixtures(object):
    """
    Scrap game fixtures.csv from the web
    with different leagues having there on function
    """
    def __init__(self):
        self.league_file = None
        self.log = log

    def fetch_all_league_fixtures(self, league):
        """
        :return: game fixtures.csv
        """
        self.log.info("Getting {} league fixture".format(league))
        data = pd.read_csv(get_analysis_root_path('prototype/data/fixtures/all_fixtures/{}.csv'.format(league)),
                           usecols=['Date', 'Time', 'HomeTeam', 'AwayTeam'])
        start_date, end_date = get_start_and_end_dates(end_days=2)

        indexed_data = data.set_index(['Date'])
        indexed_data = indexed_data.loc[start_date:end_date]
        data = indexed_data.reset_index()

        team_mapping = get_config(file="team_mapping/{}".format(league))
        team_mapping = team_mapping.get(league)
        data.loc[:, "HomeTeam"] = data.HomeTeam.map(team_mapping)
        data.loc[:, "AwayTeam"] = data.AwayTeam.map(team_mapping)
        fixtures = []

        return fixtures

    def save_games_to_predict(self):
        config_dict = get_config("league")

        fixtures = []
        for key in config_dict.keys():
            self.league_file = key
            fixtures += self.fetch_all_league_fixtures(league=key)

        fixtures = pd.DataFrame(fixtures).dropna()
        self.log.info("Fixtures shape: {}".format(fixtures.shape))
        save_fixtures_to_file(fixtures, folder="selected_fixtures")


if __name__ == '__main__':
    GameFixtures().save_games_to_predict()
