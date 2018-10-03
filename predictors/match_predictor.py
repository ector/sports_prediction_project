import os
import pandas as pd
from pymongo import MongoClient
from sklearn.externals import joblib

from utils import get_analysis_root_path, get_config
from te_logger.logger import log


mongodb_uri = get_config("db").get("sport_prediction_url")


class Predictors(object):
    def __init__(self):
        self.result_map = {-3: "2", 0: "X", 3: "1"}
        self.inverse_ou_class = {1: 'O', 0: 'U'}
        self.log = log

    def treat_result(self, rst_1x, rst_x2):
        """
        Mapping results in
        :param rst_1x: data {0: '1X', 1: 'A'}
        :param rst_x2: data {0: 'X2', 1: 'H'}
        :return:
        """
        dc_map = []
        for i, j in zip(rst_1x, rst_x2):
            if [i, j] == [0, 1]:
                dc_map.append("1X")
            elif [i, j] == [1, 0]:
                dc_map.append("X2")
            else:
                dc_map.append("12")

        self.log.info("mapped result {}".format(dc_map))

        return dc_map

    def predict_winner(self, league):
        prediction = None

        wdw_columns = get_config(file="wdw_columns/{}".format(league)).get(league)
        ou25_columns = get_config(file="ou25_columns/{}".format(league)).get(league)

        lg_data = pd.read_csv(get_analysis_root_path('tools/data/clean_data/team_trend/{}.csv'.format(league)), index_col=False)
        unplayed_data = lg_data[lg_data["played"] == 0]

        if len(unplayed_data) > 0:
            wdw_data = unplayed_data[wdw_columns]
            ou25_data = unplayed_data[ou25_columns]

            try:
                # For wdw Market
                clf = joblib.load(get_analysis_root_path('tools/league_models/{}'.format(league)))

                unplayed_data.loc[:, "p_ftr"] = clf.predict(wdw_data)
                unplayed_data.loc[:, "league"] = league

                outcome_probs = clf.predict_proba(wdw_data)
                unplayed_data.loc[:, "d_prob"] = outcome_probs[:, 1]
                unplayed_data.loc[:, "a_prob"] = outcome_probs[:, 0]
                unplayed_data.loc[:, "h_prob"] = outcome_probs[:, 2]
                # For Double Chance Market
                clf_1x = joblib.load(get_analysis_root_path('tools/league_models/{}_1x'.format(league)))
                clf_x2 = joblib.load(get_analysis_root_path('tools/league_models/{}_x2'.format(league)))

                unplayed_data.loc[:, "dc"] = self.treat_result(clf_1x.predict(wdw_data), clf_x2.predict(wdw_data))

                # For O/U 2.5 Market
                ou25_model = joblib.load(get_analysis_root_path('tools/league_models/{}_ou25'.format(league)))
                unplayed_data.loc[:, "ou25"] = ou25_model.predict(ou25_data)

                prediction = unplayed_data[["Date", "Time", "HomeTeam", "AwayTeam", "p_ftr", "d_prob", "a_prob", "h_prob",
                                            "dc", "ou25", "league"]]
                prediction.rename(index=str, columns={"Date": "date", "Time": "time", "HomeTeam": "home",
                                                      "AwayTeam":"away", "p_ftr": "prediction"}, inplace=True)
                team_mapping = get_config(file="team_mapping/{}".format(league)).get(league)
                team_mapping_inv = {v: k for k, v in team_mapping.items()}

                prediction["home"].replace(team_mapping_inv, inplace=True)
                prediction["away"].replace(team_mapping_inv, inplace=True)
                prediction["prediction"].replace(self.result_map, inplace=True)
                prediction["ou25"].replace(self.inverse_ou_class, inplace=True)

            except Exception as e:
                self.log.error(msg="The following error occurred: {}".format(e))

        return prediction

    def save_prediction(self, league):

        match_predictions = self.predict_winner(league=league)

        if match_predictions is not None:
            self.log.info("{} prediction dataframe sorted by date, time and league".format(league))
            preds = match_predictions.sort_values(['date', 'time', 'league'])

            pred_list = []

            try:
                self.log.info("Connecting to the database")
                client = MongoClient(mongodb_uri + "22", connectTimeoutMS=30000)
                db = client.get_database("sports_prediction")

                wdw_football = db.wdw_football

                self.log.info("Inserting predictions")
                for idx, pred in preds.iterrows():

                    pred = dict(pred)
                    exist = {'league': pred.get('league'), 'home': pred.get('home'), 'away': pred.get('away'),
                         'time': pred.get('time'), 'date': pred.get('date')}
                    wdw_count = wdw_football.count_documents(exist)

                    if wdw_count == 0:
                        pred_list.append(pred)
                    elif wdw_count == 1:
                        wdw_football.update_one(exist, {'$set': pred})

                if len(pred_list) != 0:
                    wdw_football.insert_many(pred_list)
                self.log.info("Done!!!")
            except Exception as e:
                self.log.error("Saving to wdw: \n{0}".format(str(e)))
                preds.to_csv(get_analysis_root_path('tools/data/predictions/wdw_{}'.format(league)),
                             index=False)


if __name__ == '__main__':
    dr = Predictors()
    for lg in get_config().keys():
        if os.path.exists(get_analysis_root_path('tools/data/fixtures/selected_fixtures/{}.csv'.format(lg))):
            dr.save_prediction(league=lg)
        else:
            log.warning("{} has no new games".format(lg).upper())
