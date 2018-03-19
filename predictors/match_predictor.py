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
from tools.utils import get_analysis_root_path, get_config, encode_data

model_columns = get_config(file="model_columns")

used_col = ['HomeTeam', 'AwayTeam', 'Date', 'HomeLastWin', 'AwayLastWin', 'HomeLastTrend', 'AwayLastTrend',
            'HomeLast3Games', 'AwayLast3Games', 'HomeLast5Games', 'AwayLast5Games', 'AwayTrend', 'HomeTrend',
            'HomeAveG', 'AwayAveG', 'HomeAveGC', 'AwayAveGC', "HomeAveHomeG", "AwayAveAwayG", 'Home5HomeTrend', 'Away5AwayTrend']

mongodb_uri = get_config("db").get("sport_prediction_url")


class Predictors(object):
    def __init__(self):
        self.result_map = {1: "X", 2: "2", 3: "1"}
        self.inverse_ou_class = {1: 'O', 0: 'U'}
        self.inverse_1x = {0: '1X', 1: 'A'}
        self.inverse_x2 = {0: 'X2', 1: 'H'}
        self.log = log
        self.process_data = ProcessData()
        self.football_data = GetFootballData()
        self.home_draw_away_suite = DeriveFootballFeatures()
        self.cps = CleanProcessStore()

    def treat_result(self, rst_1x, rst_x2):
        results = [self.inverse_1x.get(rst_1x), self.inverse_x2.get(rst_x2)]

        if results == ["1X", "H"]:
            mapped = "1X"
        elif results == ["A", "X2"]:
            mapped = "X2"
        else:
            mapped = "12"

        self.log.info("mapped result {}".format(mapped))

        return mapped

    def predict_winner(self):
        used_col.append("Time")
        used_col.append("League")

        match_predictions = []

        fixt = pd.read_csv(
            get_analysis_root_path('prototype/data/fixtures/fixtures_team_trend/fixtures_team_trend.csv'),
            usecols=used_col, index_col=False)
        # try:
            # Added this to
            # fixt = fixt.dropna()

        for idx, game_list in fixt.iterrows():
            try:
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

                self.log.info("Loading data for wdw and double chance markets")
                clmns = joblib.load(get_analysis_root_path('prototype/league_models/{}_cols'.format(league)))

                team_mapping = joblib.load(get_analysis_root_path('prototype/league_models/{}_stdsc'.format(league)))

                next_game_data = next_game.drop("Date", axis=1)
                next_game_data = encode_data(data=next_game_data, team_mapping=team_mapping)

                np.set_printoptions(precision=3)

                for col in clmns:
                    if col not in list(next_game_data.columns):
                        next_game_data[col] = 0

                next_game_data = next_game_data[clmns]

                # For wdw Market
                clf = joblib.load(get_analysis_root_path('prototype/league_models/{}'.format(league)))
                # For Double Chance Market
                clf_1x = joblib.load(get_analysis_root_path('prototype/league_models/{}_1x'.format(league)))
                clf_x2 = joblib.load(get_analysis_root_path('prototype/league_models/{}_x2'.format(league)))

                outcome_probs = list(clf.predict_proba(next_game_data)[0])

                dc = self.treat_result(clf_1x.predict(next_game_data)[0], clf_x2.predict(next_game_data)[0])

                game_prediction = {"prediction": self.result_map.get(clf.predict(next_game_data)[0]),
                                   "d_prob": "{:.2%}".format(outcome_probs[0]), "a_prob": "{:.2%}".format(outcome_probs[1]),
                                   "h_prob": "{:.2%}".format(outcome_probs[2]),
                                   'outcome_probs': outcome_probs, 'date': game_list.get('Date'),
                                   'time': game_time, 'home': game_list.get('HomeTeam'), 'away': game_list.get('AwayTeam'),
                                   'league': league,
                                   'ou25': self.inverse_ou_class.get(ou_model.predict(ou_data)[0]),
                                   'dc': dc}
                self.log.info("Game prediction: {}".format(game_prediction))
                game_pred = game_prediction.copy()
                match_predictions.append(game_pred)

            except Exception as e:
                self.log.error(msg="The following error occurred: {}".format(e))

        return match_predictions

    def save_prediction(self):
        pred_cols = ['date', 'time', 'home', 'away', 'prediction', 'd_prob', 'a_prob', 'h_prob', 'outcome_probs',
                   'league', 'ou25', 'dc']

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
