import json

import numpy as np
import pandas as pd
from sklearn.externals import joblib
from sklearn.preprocessing import StandardScaler

from tools.clean_process_and_store import CleanProcessStore
from te_logger.logger import MyLogger
from tools.home_draw_away_suite import DeriveFootballFeatures
from tools.process_data import ProcessData, GetFootballData
from tools.utils import get_analysis_root_path, get_config

pd.set_option("display.max_rows", 250)


class Predictors(MyLogger):
    def __init__(self):
        MyLogger.logger(self)
        self.process_data = ProcessData()
        self.football_data = GetFootballData()
        self.home_draw_away_suite = DeriveFootballFeatures()
        self.cps = CleanProcessStore()
        # self.league_analysis = None
        self.pass_rate = json.loads(get_config("pass_rate"))

    def predict_winner(self):
        fixt = pd.read_csv(get_analysis_root_path('prototype/data/fixtures/fixtures'),
                           usecols=['Date', 'Time', 'HomeTeam', 'AwayTeam', 'League'], index_col=False)

        match_predictions = []
        unique_leagues = list(np.unique(fixt.League.values))

        for lg in unique_leagues:
            each_fixt_league = fixt[fixt.League == lg]
            clmns = self.pass_rate.get(lg).get('attr')

            stdsc = StandardScaler()
            stdsc_data = joblib.load(get_analysis_root_path('prototype/league_models/{}_stdsc'.format(lg)))
            stdsc_clf = stdsc.fit(stdsc_data)

            for game_list in each_fixt_league.itertuples(index=False):

                league = game_list[4]
                game_time = game_list[1]
                game = {'HomeTeam': game_list[2],
                        'AwayTeam': game_list[3],
                        'Date': game_list[0],
                        'HomeLastWin': 0,
                        'AwayLastWin': 0,
                        'HomeLast3Games': 0,
                        'AwayLast3Games': 0,
                        'HomeLast5Games': 0,
                        'AwayLast5Games': 0}

                trend_columns = ['Date', 'HomeLastWin', 'AwayLastWin', 'HomeLast3Games', 'AwayLast3Games',
                                 'HomeLast5Games', 'AwayLast5Games']
                # Get the last game result
                next_game = self.compute_single_game_home_and_away_last_win_data(league=league, game=game)
                next_game = pd.DataFrame(next_game, index=[0])

                next_game_data = pd.get_dummies(next_game[['HomeTeam', 'AwayTeam']])

                for col in clmns:
                    if col not in list(next_game_data.columns):
                        next_game_data[col] = 0

                next_game_data[trend_columns] = next_game[trend_columns]

                next_game_data = next_game_data[list(next_game_data.columns)]

                next_game_data_w_dateindex = next_game_data.set_index('Date')

                try:
                    stdsc_game_data = stdsc_clf.transform(next_game_data_w_dateindex)
                    game_prediction = self.predict_next_game(league=lg, next_game=stdsc_game_data)
                except:
                    game_prediction = {"prediction": '-',  "outcome_probs": [[0, 0, 0]][0]}

                game_prediction['date'] = game.get('Date')
                game_prediction['time'] = game_time
                game_prediction['home'] = game.get('HomeTeam')
                game_prediction['away'] = game.get('AwayTeam')
                game_prediction['league'] = league
                self.log.info("Game prediction: {}".format(game_prediction))
                match_predictions.append(game_prediction)
        return match_predictions

    def compute_single_game_home_and_away_last_win_data(self, league, game):
        """
        Compute home and away last game result for a game
        :param league: string
        :param game: dict
        :return: dict
        """
        data = self.football_data.get_football_data(league=league)

        away_data = data[(data.AwayTeam == game.get('AwayTeam')) | (data.HomeTeam == game.get('AwayTeam'))]

        # Create keys and values for columns
        self.cps.column_dict = {value: key for key, value in enumerate(list(data.columns))}
        self.log.info("Column dict: \n{}".format(self.cps.column_dict))

        # For away Team
        try:
            away_last_data = away_data.tail(1).values.tolist()[0]
            away_last_3_data = away_data.tail(3).values.tolist()[:3]
            away_last_5_data = away_data.tail(5).values.tolist()[:5]
        except IndexError:
            away_last_data = [game.get('HomeTeam'), '2017-05-21', 'A', game.get('AwayTeam'), 1617, 3, 3, 3, 3, 3, 3]
            away_last_3_data = [[game.get('HomeTeam'), '2017-05-21', 'A', game.get('AwayTeam'), 1617, 3, 3, 3, 3, 3, 3]]
            away_last_5_data = [[game.get('HomeTeam'), '2017-05-21', 'A', game.get('AwayTeam'), 1617, 3, 3, 3, 3, 3, 3]]

        # For home team
        home_data = data[(data.AwayTeam == game.get('HomeTeam')) | (data.HomeTeam == game.get('HomeTeam'))]
        try:
            home_last_data = home_data.tail(1).values.tolist()[0]
            home_last_3_data = home_data.tail(3).values.tolist()[:3]
            home_last_5_data = home_data.tail(5).values.tolist()[:5]
        except IndexError:
            home_last_data = [game.get('AwayTeam'), '2017-05-21', 'A', game.get('HomeTeam'), 1617, 3, 3, 3, 3, 3, 3]
            home_last_3_data = [[game.get('AwayTeam'), '2017-05-21', 'A', game.get('HomeTeam'), 1617, 3, 3, 3, 3, 3, 3]]
            home_last_5_data = [[game.get('AwayTeam'), '2017-05-21', 'A', game.get('HomeTeam'), 1617, 3, 3, 3, 3, 3, 3]]

        game['HomeLastWin'] = self.cps.win_loss(array_list=home_last_data, team=game.get('HomeTeam'))
        game['AwayLastWin'] = self.cps.win_loss(array_list=away_last_data, team=game.get('AwayTeam'))
        game['HomeLast3Games'] = self.cps.last_games_trend(array_list=home_last_3_data, team=game.get('HomeTeam'))
        game['AwayLast3Games'] = self.cps.last_games_trend(array_list=away_last_3_data, team=game.get('AwayTeam'))
        game['HomeLast5Games'] = self.cps.last_games_trend(array_list=home_last_5_data, team=game.get('HomeTeam'))
        game['AwayLast5Games'] = self.cps.last_games_trend(array_list=away_last_5_data, team=game.get('AwayTeam'))

        print("Formed data: {}".format(game))
        return game

    def predict_next_game(self, league, next_game):
        """
        Predict the game 
        :param league: string
        :param next_game: dict
        :return: prediction outcome
        """
        self.inverse_ftr_class = {1: 'D', 2: 'A', 3: 'H'}

        clf = joblib.load(get_analysis_root_path('prototype/league_models/{}'.format(league)))

        default_out_proba = [[0, 0, 0]]
        try:
            out_proba = clf.predict_proba(next_game)
        except:
            out_proba = default_out_proba[0]

        try:
            np.set_printoptions(precision=3)
            outcome = {"prediction": self.inverse_ftr_class.get(clf.predict(next_game)[0]),
                       "outcome_probs": out_proba[0]}
        except:
            outcome = {"prediction": '-',
                       "outcome_probs": default_out_proba[0]}
        return outcome

    def save_prediction(self):
        match_predictions = self.predict_winner()
        preds = pd.DataFrame(match_predictions, columns=['date', 'time', 'home', 'away', 'prediction', 'outcome_probs', 'league'])
        preds = preds.sort_values(['date', 'time', 'league'])
        self.log.info("Saving to wdw")
        preds.to_csv(get_analysis_root_path('sports_betting/predictions/wdw'), columns=['date', 'time', 'home', 'away', 'prediction','outcome_probs', 'league'], index=False)


if __name__ == '__main__':
    dr = Predictors()
    dr.save_prediction()
