import os
import pandas as pd
from pymongo import MongoClient
from sklearn.externals import joblib

from utils import get_analysis_root_path, get_config
from te_logger.logger import log


mongodb_uri = get_config("db").get("sport_prediction_url")


def unplayed_games(league: str, market: str):

    columns = get_config(file="{}_columns/{}".format(market, league))

    lg_data = pd.read_csv(get_analysis_root_path('tools/data/clean_data/team_trend/{}.csv'.format(league)),
                          index_col=False)
    unplayed = lg_data[lg_data["played"] == 0]

    if len(unplayed) > 0:
        return unplayed, columns
    return None


class Predictors(object):
    def __init__(self):
        self.result_map = {1: "A", 0: "X", 2: "H"}
        self.inverse_ou_class = {1: 'O', 0: 'U'}
        self.log = log
        self.unplayed_df = unplayed_games

    def predict(self, league, market):
        print(league , market)
        unplayed_data_columns = self.unplayed_df(league=league, market=market)
        if unplayed_data_columns is None:
            return None
        unplayed_data= unplayed_data_columns[0]
        columns = unplayed_data_columns[1]
        unplayed_df = unplayed_data[columns]


        try:
            clf = joblib.load(get_analysis_root_path('tools/league_models/{}_{}'.format(league, market)))

            unplayed_data.loc[:, market] = clf.predict(unplayed_df)

            if market == "wdw":
                outcome_probs = clf.predict_proba(unplayed_df)
                unplayed_data.loc[:, "d_prob"] = outcome_probs[:, 0]
                unplayed_data.loc[:, "a_prob"] = outcome_probs[:, 1]
                unplayed_data.loc[:, "h_prob"] = outcome_probs[:, 2]
                unplayed_data[market].replace(self.result_map, inplace=True)
            elif market == "ou25":
                unplayed_data.loc[:, market].replace(self.inverse_ou_class, inplace=True)
            elif market == "dc":
                unplayed_data.loc[:, market].replace({0: '1X', 1: '12'}, inplace=True)

            unplayed_data.loc[:, "league"] = league

            prediction = unplayed_data[["Date", "Time", "HomeTeam", "AwayTeam", market, "league"]]
            prediction.rename(index=str, columns={"Date": "date", "Time": "time", "HomeTeam": "home",
                                                  "AwayTeam": "away"}, inplace=True)
            team_mapping = get_config(file="team_mapping/{}".format(league))
            team_mapping_inv = {v: k for k, v in team_mapping.items()}

            prediction["home"].replace(team_mapping_inv, inplace=True)
            prediction["away"].replace(team_mapping_inv, inplace=True)
            return prediction

        except Exception as e:
            self.log.error(msg="The following error occurred: {}".format(e))
            return

    def save_prediction(self, league, market):

        match_predictions = self.predict(league=league, market=market)

        if match_predictions is not None:
            self.log.info("{} prediction dataframe sorted by date, time and league".format(league))
            preds = match_predictions.sort_values(['date', 'time', 'league'])
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
                    wdw_count = wdw_football.count_documents(exist)

                    if wdw_count == 0:
                        pred_list.append(pred)
                    elif wdw_count == 1:
                        wdw_football.update_one(exist, {'$set': pred})

                if len(pred_list) != 0:
                    wdw_football.insert_many(pred_list)
                self.log.info("Done!!!")
            except Exception as e:
                self.log.error("Could not save {} {} into the database: \n{}".format(league, market, str(e)))
                preds.to_csv(get_analysis_root_path('tools/data/predictions/{}_{}.csv'.format(league, market)),
                             index=False)


if __name__ == '__main__':
    dr = Predictors()
    for lg in get_config().keys():
        if os.path.exists(get_analysis_root_path('tools/data/fixtures/selected_fixtures/{}.csv'.format(lg))):
            for mkt in ["wdw", "dc", "ou25"]:
                dr.save_prediction(league=lg, market=mkt)
        else:
            log.warning("{} has no new games".format(lg).upper())
