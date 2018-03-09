import numpy as np
import pandas as pd
from copy import deepcopy
from pymongo import MongoClient
from sklearn.externals import joblib
from sklearn.preprocessing import StandardScaler

from te_logger.logger import log
from tools.clean_process_and_store import CleanProcessStore
from tools.home_draw_away_suite import DeriveFootballFeatures
from tools.process_data import ProcessData, GetFootballData
from tools.utils import get_analysis_root_path, get_config

model_columns = get_config(file="model_columns")

used_col = ['HomeTeam', 'AwayTeam', 'Date', 'HomeLastWin', 'AwayLastWin', 'HomeLastTrend', 'AwayLastTrend',
            'HomeLast3Games', 'AwayLast3Games', 'HomeLast5Games', 'AwayLast5Games', 'AwayTrend', 'HomeTrend',
            'HomeAveG', 'AwayAveG', 'HomeAveGC', 'AwayAveGC', "HomeAveHomeG", "AwayAveAwayG", 'Home5HomeTrend', 'Away5AwayTrend']

mongodb_uri = get_config("db").get("sport_prediction_url")


class Predictors(object):
    def __init__(self):
        self.result_map = {0: "X", 1: "2", 2: "1"}
        self.inverse_ou_class = {1: 'O', 0: 'U'}
        self.log = log
        self.process_data = ProcessData()
        self.football_data = GetFootballData()
        self.home_draw_away_suite = DeriveFootballFeatures()
        self.cps = CleanProcessStore()

    def treat_result(self, result):
        c = deepcopy(result)
        d = deepcopy(result)

        max_one = d.index(max(d))
        c[max_one] = 0.0
        max_two = c.index(max(c))

        if (result[max_one] - result[max_two]) <= 0.2:
            result_index = sorted([max_one, max_two])
        else:
            result_index = [max_one]

        self.log.info("result indices {}".format(result_index))

        mapped = "".join([self.result_map.get(i) for i in result_index])

        if mapped in ["X1", "21"]:
            mapped = mapped[::-1]

        self.log.info("mapped result {}".format(mapped))

        return mapped

    def predict_winner(self):
        used_col.append("Time")
        used_col.append("League")

        match_predictions = []

        try:
            fixt = pd.read_csv(get_analysis_root_path('prototype/data/fixtures/fixtures_team_trend/fixtures_team_trend.csv'),
                               usecols=used_col, index_col=False)
            # Added this to
            # fixt = fixt.dropna()

            for idx, game_list in fixt.iterrows():
                game_list = dict(game_list)
                league = game_list.pop("League")
                game_time = game_list.pop("Time")

                # Get the last game result
                next_game = pd.DataFrame([game_list])
                ou_data = next_game.copy()

                # For Over and Under Market
                self.log.info("Loading data for over or under market")
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
                self.log.info("Loading data for win draw win market")
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

                game_prediction = {"prediction": self.treat_result(outcome_probs),
                                   "d_prob": "{:.2%}".format(outcome_probs[0]), "a_prob": "{:.2%}".format(outcome_probs[1]),
                                   "h_prob": "{:.2%}".format(outcome_probs[2]),
                                   'outcome_probs': list(clf.predict_proba(stdsc_game_data)[0]), 'date': game_list.get('Date'),
                                   'time': game_time, 'home': game_list.get('HomeTeam'), 'away': game_list.get('AwayTeam'),
                                   'league': league,
                                   'ou25': self.inverse_ou_class.get(ou_model.predict(ou_data)[0])}
                self.log.info("Game prediction: {}".format(game_prediction))
                game_pred = game_prediction.copy()
                match_predictions.append(game_pred)

        except Exception as e:
            self.log.error(msg="The following error occurred: {}".format(e))

        return match_predictions

    def save_prediction(self):
        pred_cols = ['date', 'time', 'home', 'away', 'prediction', 'd_prob', 'a_prob', 'h_prob', 'outcome_probs',
                   'league', 'ou25']

        match_predictions = self.predict_winner()

        self.log.info("Sort prediction dataframe by date, time and league")
        preds = pd.DataFrame(match_predictions, columns=pred_cols)
        preds = preds.sort_values(['date', 'time', 'league'])

        pred_list = []

        try:
            self.log.info("Connecting to the database")
            client = MongoClient(mongodb_uri, connectTimeoutMS=30000)
            db = client.get_database("sports_prediction")

            wdw_football = db.wdw_football

            self.log.info("Inserting predictions")
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
            self.log.info("Done!!!")
        except Exception as e:
            self.log.error("Saving to wdw: \n{0}".format(str(e)))
            preds.to_csv(get_analysis_root_path('sports_betting/predictions/wdw'),
                         columns=pred_cols, index=False)


if __name__ == '__main__':
    dr = Predictors()
    dr.save_prediction()
