import pandas as pd

from te_logger.logger import log
from utils import get_analysis_root_path, get_start_and_end_dates, get_config, save_fixtures_to_file, team_translation, \
    delete_fixtures_in_file

translation = get_config("team_translation")


class GameFixtures(object):
    """
    Scrap game fixtures.csv from the web
    with different leagues having there on function
    """
    def __init__(self):
        self.league_file = None
        self.log = log
        self.days = 3

    def fetch_all_league_fixtures(self, league):
        """
        :return: game fixtures.csv
        """
        self.log.info("Getting {} league fixture".format(league))
        data = pd.read_csv(get_analysis_root_path('tools/data/fixtures/all_fixtures/{}.csv'.format(league)),
                           usecols=['Date', 'Time', 'HomeTeam', 'AwayTeam'])

        data = team_translation(data=data, league=league)
        start_date, end_date = get_start_and_end_dates(end_days=self.days)

        indexed_data = data.set_index(['Date'])
        indexed_data = indexed_data.loc[start_date:end_date]
        data = indexed_data.reset_index()

        return data

    def save_games_to_predict(self):
        config_dict = get_config("league")

        for league in config_dict.keys():
            self.league_file = league
            fixtures = self.fetch_all_league_fixtures(league=league)
            fixtures = pd.DataFrame(fixtures).dropna()
            if len(fixtures) != 0:
                self.log.info("{} Fixtures shape: {}".format(league.upper(), fixtures.shape))
                save_fixtures_to_file(fixtures, folder="selected_fixtures/{}".format(league))
            else:
                delete_fixtures_in_file(folder="selected_fixtures/{}".format(league))
                self.log.warn("{} has no game in the next {} days".format(league.upper(), self.days))


if __name__ == '__main__':
    GameFixtures().save_games_to_predict()
