import json
from difflib import get_close_matches

import numpy as np
import pandas as pd

from te_logger.logger import log
# from tools import ProcessData
# from tools import get_config, save_fixtures_to_file, get_analysis_root_path, \
#     get_start_and_end_dates
from tools.process_data import ProcessData
from tools.utils import get_analysis_root_path, get_start_and_end_dates, get_config, save_fixtures_to_file


class GameFixtures(object):
    """
    Scrap game fixtures.csv from the web
    with different leagues having there on function
    """
    def __init__(self):
        self.league_file = None
        self.log = log

    def fetch_all_league_fixtures(self):
        """
        Scrap fixtures.csv from BBC premiership fixtures.csv page
        :return: game fixtures.csv
        """
        data = pd.read_csv(get_analysis_root_path('prototype/data/fixtures/all_fixtures/{}.csv'.format(self.league_file)),
                           usecols=['Date', 'Time', 'HomeTeam', 'AwayTeam'])
        start_date, end_date = get_start_and_end_dates(end_days=2)
        teams = ProcessData().get_team_names(league=self.league_file)

        indexed_data = data.set_index(['Date'])
        indexed_data = indexed_data.loc[start_date:end_date]
        df = indexed_data.reset_index()
        fixtures = []

        for game_date, game_time, home, away in zip(df.Date.values, df.Time.values, df.HomeTeam.values, df.AwayTeam.values):
            try:
                translated_home = get_close_matches(home, teams)[0]
                translated_away = get_close_matches(away, teams)[0]
                fix = {"Date": game_date, "Time": game_time, "HomeTeam": translated_home,
                                 "AwayTeam": translated_away, "League": self.league_file, "away": away, "home": home}

                # Added home and away keys for debugging purpose
                self.log.warn(msg="{}".format(fix))
                fix.pop("home")
                fix.pop("away")
                fixtures.append(fix)
            except IndexError:
                self.log.error("No data for either {} or {}".format(home, away))

        return fixtures

    def save_games_to_predict(self):
        config_dict = get_config("league")

        fixtures = []
        for key in config_dict.keys():
            self.league_file = key
            fixtures += self.fetch_all_league_fixtures()

        fixtures = pd.DataFrame(fixtures).dropna()
        self.log.info("Fixtures shape: {}".format(fixtures.shape))
        save_fixtures_to_file(fixtures, folder="selected_fixtures")


if __name__ == '__main__':
    GameFixtures().save_games_to_predict()
