import numpy as np
import pandas as pd
from pymongo import MongoClient
from sklearn.externals import joblib
from sklearn.preprocessing import StandardScaler

from te_logger.logger import log
from tools.clean_process_and_store import CleanProcessStore
from tools.home_draw_away_suite import DeriveFootballFeatures
from tools.process_data import ProcessData, GetFootballData
from tools.utils import get_analysis_root_path, get_config

pd.set_option("display.max_rows", 250)


model_columns = get_config(file="model_columns")

used_col = ['HomeTeam', 'AwayTeam', 'Date', 'HomeLastWin', 'AwayLastWin', 'HomeLastTrend', 'AwayLastTrend',
            'HomeLast3Games', 'AwayLast3Games', 'HomeLast5Games', 'AwayLast5Games', 'AwayTrend', 'HomeTrend',
            'HomeAveG', 'AwayAveG', 'HomeAveGC', 'AwayAveGC', "HomeAveHomeG", "AwayAveAwayG", 'Home5HomeTrend', 'Away5AwayTrend']

mongodb_uri = get_config("db").get("sport_prediction_url")


class Predictors(object):
    def __init__(self):
        self.inverse_ftr_class = {1: 'D', 2: 'A', 3: 'H'}
        self.inverse_ou_class = {1: 'O', 0: 'U'}
        self.log = log
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

            # Get the last game result
            next_game = pd.DataFrame([game_list])
            ou_data = next_game.copy()

            # For Over and Under Market
            ou_cols = list(get_config("model_columns").get("over_under_25_cols"))
            team_map = joblib.load(get_analysis_root_path('prototype/league_models/{}_map'.format(league)))
            ou_model = joblib.load(get_analysis_root_path('prototype/league_models/{}_ou25'.format(league)))

            ou_cols.remove('FTHG')
            ou_cols.remove('FTAG')
            ou_cols.remove('Date')
            ou_cols.remove('Season')
            ou_data = ou_data[ou_cols]
            ou_data["HomeTeam"] = ou_data.HomeTeam.map(team_map)
            ou_data["AwayTeam"] = ou_data.AwayTeam.map(team_map)

            # For wdw Market
            clmns = joblib.load(get_analysis_root_path('prototype/league_models/{}_cols'.format(league)))

            stdsc = StandardScaler()
            stdsc_data = joblib.load(get_analysis_root_path('prototype/league_models/{}_stdsc'.format(league)))
            stdsc = stdsc.fit(stdsc_data)


            # next_game_data_wd = next_game.set_index('Date')
            next_game_data = next_game.drop("Date", axis=1)
            next_game_data = pd.get_dummies(next_game_data)

            clf = joblib.load(get_analysis_root_path('prototype/league_models/{}'.format(league)))
            np.set_printoptions(precision=3)

            for col in clmns:
                if col not in list(next_game_data.columns):
                    next_game_data[col] = 0

            next_game_data = next_game_data[clmns]

            stdsc_game_data = stdsc.transform(next_game_data)

            outcome_probs = list(clf.predict_proba(stdsc_game_data)[0])

            game_prediction = {"prediction": self.inverse_ftr_class.get(clf.predict(stdsc_game_data)[0]),
                               "d_prob": "{:.2%}".format(outcome_probs[0]), "a_prob": "{:.2%}".format(outcome_probs[1]),
                               "h_prob": "{:.2%}".format(outcome_probs[2]),
                               'outcome_probs': list(clf.predict_proba(stdsc_game_data)[0]), 'date': game_list.get('Date'),
                               'time': game_time, 'home': game_list.get('HomeTeam'), 'away': game_list.get('AwayTeam'),
                               'league': league,
                               'ou25': self.inverse_ou_class.get(ou_model.predict(ou_data)[0])}
            self.log.info("Game prediction: {}".format(game_prediction))
            game_pred = game_prediction.copy()
            match_predictions.append(game_pred)
        return match_predictions

    def save_prediction(self):
        pred_cols = ['date', 'time', 'home', 'away', 'prediction', 'd_prob', 'a_prob', 'h_prob', 'outcome_probs',
                   'league', 'ou25']
        match_predictions = self.predict_winner()
        preds = pd.DataFrame(match_predictions, columns=pred_cols)
        preds = preds.sort_values(['date', 'time', 'league'])

        pred_list = []

        try:
            client = MongoClient(mongodb_uri, connectTimeoutMS=30000)
            db = client.get_database("sports_prediction")

            wdw_football = db.wdw_football

            for idx, pred in preds.iterrows():
                pred = dict(pred)
                exist = {'league': pred.get('league'), 'home': pred.get('home'), 'away': pred.get('away'),
                     'time': pred.get('time'), 'date': pred.get('date')}
                wdw_count = wdw_football.find(exist).count()

                if wdw_count == 0:
                    pred_list.append(pred)
                elif wdw_count == 1:
                    wdw_football.update_one(exist, {'$set': pred})

            if len(pred_list) != 0:
                wdw_football.insert_many(pred_list)

        except Exception as e:
            self.log.error("Saving to wdw: \n{0}".format(str(e)))
            preds.to_csv(get_analysis_root_path('sports_betting/predictions/wdw'),
                         columns=pred_cols, index=False)


if __name__ == '__main__':
    dr = Predictors()
    dr.save_prediction()
