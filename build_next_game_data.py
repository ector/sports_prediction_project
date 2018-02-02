# -*- coding: utf-8 -*-
"""
Created on 28-11-2017 at 9:16 PM 

@author: tola
"""
import pandas as pd

from te_logger.logger import log
from tools.clean_process_and_store import CleanProcessStore
from tools.home_draw_away_suite import DeriveFootballFeatures
from tools.process_data import ProcessData, GetFootballData
from tools.utils import get_analysis_root_path, get_config, save_fixtures_to_file

pd.set_option("display.max_rows", 2500)


class FixturesStanding(object):
    def __init__(self):
        self.log = log
        self.process_data = ProcessData()
        self.football_data = GetFootballData()
        self.home_draw_away_suite = DeriveFootballFeatures()
        self.cps = CleanProcessStore()

    def fixture_last_win(self):
        file_path = get_analysis_root_path('prototype/data/fixtures/selected_fixtures/selected_fixtures.csv')
        data = pd.read_csv(file_path, usecols=['Date', 'Time', 'HomeTeam', 'AwayTeam', 'League'])
        self.log.info("{}".format(data))

        new_data = []
        for idx, dt in data.iterrows():
            dt = dict(dt)
            # pull historical last_win data
            hist_data = self.football_data.get_football_last_win_data(league=dt.get("League"))

            # only need the last 40 games
            hist_data = hist_data.tail(40)

            away_data = hist_data[(hist_data.AwayTeam == dt.get('AwayTeam')) | (hist_data.HomeTeam == dt.get('AwayTeam'))]

            # For away Team
            away_last_data = away_data.tail(1)

            # For home team
            home_data = hist_data[(hist_data.AwayTeam == dt.get('HomeTeam')) | (hist_data.HomeTeam == dt.get('HomeTeam'))]
            home_last_data = home_data.tail(1)

            dt['HomeLastWin'] = self.cps.fixtures_last_game_win_draw_loss(last_data=home_last_data, team=dt.get('HomeTeam'))
            dt['AwayLastWin'] = self.cps.fixtures_last_game_win_draw_loss(last_data=away_last_data, team=dt.get('AwayTeam'))

            self.log.info("Formed data: {}".format(dt))

            new_data.append(dt)

        new_data = pd.DataFrame(new_data)
        trend_map = {1: "D", 0: "L", 3: "W"}
        new_data['HomeLastTrend'] = new_data.HomeLastWin.map(trend_map)
        new_data['AwayLastTrend'] = new_data.AwayLastWin.map(trend_map)
        save_fixtures_to_file(data=new_data, folder="fixtures_last_win")
        return

    def fixtures_team_trend(self):
        file_path = get_analysis_root_path('prototype/data/fixtures/fixtures_last_win/fixtures_last_win.csv')
        data = pd.read_csv(file_path)

        new_data = []
        for idx, dt in data.iterrows():
            dt = dict(dt)

            hist_data = self.football_data.get_football_last_win_data(dt.get('League'))

            # Using the last 160 games
            hist_data = hist_data.tail(160)

            ldw = {0: "L", 1: "D", 3: "W"}
            hist_data['HomeLastTrend'] = hist_data.HomeLastWin.map(ldw)
            hist_data['AwayLastTrend'] = hist_data.AwayLastWin.map(ldw)

            # For away Team
            away_data = hist_data[(hist_data.AwayTeam == dt.get('AwayTeam')) | (hist_data.HomeTeam == dt.get('AwayTeam'))]
            away_last_data = away_data.tail(11)
            away_last_3_data = away_data.tail(3)
            away_last_5_data = away_data.tail(5)

            # For home team
            home_data = hist_data[(hist_data.AwayTeam == dt.get('HomeTeam')) | (hist_data.HomeTeam == dt.get('HomeTeam'))]
            home_last_data = home_data.tail(11)
            home_last_3_data = home_data.tail(3)
            home_last_5_data = home_data.tail(5)

            dt['AwayLast3Games'] = self.cps.fixtures_last_games_trend(n_data=away_last_3_data, team=dt.get('AwayTeam'))
            dt['HomeLast3Games'] = self.cps.fixtures_last_games_trend(n_data=home_last_3_data, team=dt.get('HomeTeam'))

            dt['AwayLast5Games'] = self.cps.fixtures_last_games_trend(n_data=away_last_5_data, team=dt.get('AwayTeam'))
            dt['HomeLast5Games'] = self.cps.fixtures_last_games_trend(n_data=home_last_5_data, team=dt.get('HomeTeam'))

            dt['AwayTrend'] = self.cps.fixtures_team_games_trend(n_data=away_last_5_data, team=dt.get('AwayTeam'))
            dt['HomeTrend'] = self.cps.fixtures_team_games_trend(n_data=home_last_5_data, team=dt.get('HomeTeam'))

            dt['AwayAveG'] = self.cps.fixtures_team_ft_ave_goals(n_data=away_last_5_data, team=dt.get('AwayTeam'))
            dt['HomeAveG'] = self.cps.fixtures_team_ft_ave_goals(n_data=home_last_5_data, team=dt.get('HomeTeam'))

            dt['AwayAveGC'] = self.cps.fixtures_team_ft_ave_goals_concided(n_data=away_last_5_data, team=dt.get('AwayTeam'))
            dt['HomeAveGC'] = self.cps.fixtures_team_ft_ave_goals_concided(n_data=home_last_5_data, team=dt.get('HomeTeam'))

            # dt['AwayAveC'] = self.cps.fixtures_team_ft_ave_corners(n_data=away_last_5_data, team=dt.get('AwayTeam'),
            #                                                        col='corner')
            # dt['HomeAveC'] = self.cps.fixtures_team_ft_ave_corners(n_data=home_last_5_data, team=dt.get('HomeTeam'),
            #                                                        col='corner')
            #
            # dt['AwayAveS'] = self.cps.fixtures_team_ft_ave_corners(n_data=away_last_5_data, team=dt.get('AwayTeam'),
            #                                                        col='shots')
            # dt['HomeAveS'] = self.cps.fixtures_team_ft_ave_corners(n_data=home_last_5_data, team=dt.get('HomeTeam'),
            #                                                        col='shots')
            #
            # dt['AwayAveST'] = self.cps.fixtures_team_ft_ave_corners(n_data=away_last_5_data, team=dt.get('AwayTeam'),
            #                                                         col='shots_on_target')
            # dt['HomeAveST'] = self.cps.fixtures_team_ft_ave_corners(n_data=home_last_5_data, team=dt.get('HomeTeam'),
            #                                                         col='shots_on_target')

            dt['AwayAveAwayG'] = self.cps.fixtures_local_and_visitor_goals(n_data=away_last_data,
                                                                           team=dt.get('AwayTeam'), location="away")
            dt['HomeAveHomeG'] = self.cps.fixtures_local_and_visitor_goals(n_data=home_last_data,
                                                                           team=dt.get('HomeTeam'), location="home")

            dt['Away5AwayTrend'] = self.cps.fixtures_team_location_h2h_trend(n_data=away_last_data,
                                                                             team=dt.get('AwayTeam'), location="away")
            dt['Home5HomeTrend'] = self.cps.fixtures_team_location_h2h_trend(n_data=home_last_data,
                                                                             team=dt.get('HomeTeam'), location="home")
            self.log.info("Fixtures {}".format(dt))
            new_data.append(dt)

        new_data = pd.DataFrame(new_data)
        save_fixtures_to_file(data=new_data, folder="fixtures_team_trend")
        return


if __name__ == "__main__":
    fs = FixturesStanding()
    fs.fixture_last_win()
    fs.fixtures_team_trend()

