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

used_col = ['HomeTeam', 'AwayTeam', 'Date', 'HomeLastWin', 'AwayLastWin', 'HomeLastTrend', 'AwayLastTrend',
            'HomeLast3Games', 'AwayLast3Games', 'HomeLast5Games', 'AwayLast5Games', 'AwayTrend', 'HomeTrend',
            'HomeAveG', 'AwayAveG', 'HomeAveGC', 'AwayAveGC']


class Predictors(MyLogger):
    def __init__(self):
        self.inverse_ftr_class = {1: 'D', 2: 'A', 3: 'H'}
        MyLogger.logger(self)
        self.process_data = ProcessData()
        self.football_data = GetFootballData()
        self.home_draw_away_suite = DeriveFootballFeatures()
        self.cps = CleanProcessStore()

    def predict_winner(self):
        used_col.append("Time")
        used_col.append("League")
        fixt = pd.read_csv(get_analysis_root_path('prototype/data/fixtures/fixtures_team_trend/fixtures_team_trend.csv'),
                           usecols=used_col, index_col=False)

        match_predictions = []

        for idx, game_list in fixt.iterrows():
            game_list = dict(game_list)
            league = game_list.pop("League")
            game_time = game_list.pop("Time")

            clmns = joblib.load(get_analysis_root_path('prototype/league_models/{}_cols'.format(league)))

            stdsc = StandardScaler()
            stdsc_data = joblib.load(get_analysis_root_path('prototype/league_models/{}_stdsc'.format(league)))
            stdsc = stdsc.fit(stdsc_data)

            # Get the last game result
            next_game = pd.DataFrame([game_list])

            # next_game_data_wd = next_game.set_index('Date')
            next_game_data = next_game.drop("Date", axis=1)
            next_game_data = pd.get_dummies(next_game_data)

            clf = joblib.load(get_analysis_root_path('prototype/league_models/{}'.format(league)))
            np.set_printoptions(precision=10)

            for col in clmns:
                if col not in list(next_game_data.columns):
                    next_game_data[col] = 0

            next_game_data = next_game_data[clmns]

            stdsc_game_data = stdsc.transform(next_game_data)

            game_prediction = {"prediction": self.inverse_ftr_class.get(clf.predict(stdsc_game_data)[0]),
                               'outcome_probs': clf.predict_proba(stdsc_game_data)[0], 'date': game_list.get('Date'),
                               'time': game_time, 'home': game_list.get('HomeTeam'), 'away': game_list.get('AwayTeam'),
                               'league': league}
            self.log.info("Game prediction: {}".format(game_prediction))
            game_pred = game_prediction.copy()
            match_predictions.append(game_pred)
        return match_predictions

    def save_prediction(self):
        match_predictions = self.predict_winner()
        preds = pd.DataFrame(match_predictions,
                             columns=['date', 'time', 'home', 'away', 'prediction', 'outcome_probs', 'league'])
        preds = preds.sort_values(['date', 'time', 'league'])

        self.log.info("Saving to wdw")
        preds.to_csv(get_analysis_root_path('sports_betting/predictions/wdw'),
                     columns=['date', 'time', 'home', 'away', 'prediction', 'outcome_probs', 'league'], index=False)


if __name__ == '__main__':
    dr = Predictors()
    dr.save_prediction()
