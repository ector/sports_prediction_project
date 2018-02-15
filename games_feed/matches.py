import datetime

from te_logger.logger import log
from tools.games_feed.competitions import Competitions
from tools.utils import get_data_from_football_api_com, get_config, date_change
from tools.db_util.mongo_cmd import MongoConnect


class Matches(object):
    """
    Retrieve, store and get all matches provided by football-api.com
    """
    def __init__(self):
        self.log = log
        self.matches_url = "http://api.football-api.com/2.0/matches?comp_id={comp_id}&from_date=20-07-2017&to_date={to_date}&Authorization={auth}"
        self.db = MongoConnect().matches_db()
        self.auth = get_config("db").get("auth_key")
        self.competition = Competitions()

    def save_matches(self, comp_id=None):
        """
        Store matches info into database using competition id
        :return:
        """
        if comp_id is None:
            comps = self.competition.get_all_competitions()
            comps_id = [i.get('id') for i in comps]
        elif isinstance(comp_id, list):
            comps_id = comp_id
        else:
            comps_id = [comp_id]

        to_date = "{}".format(datetime.date.today())
        to_date = date_change(to_date, frm="%Y-%m-%d", to="%d-%m-%Y")

        for competition_id in comps_id:

            matches_url = self.matches_url.format(to_date=to_date, comp_id=competition_id, auth=self.auth)

            matches = get_data_from_football_api_com(url=matches_url)

            if matches is not None:

                for match in matches:
                    if '?' not in [match.get('localteam_score'), match.get('visitorteam_score'), match.get('season')]:
                        result = self.db.update({"id": match.get("id")}, match, upsert=True)
                        log.info("local mongodb, match inserted data ids: {id}".format(id=result))
            else:
                log.info("no new matches data to store")

    def get_all_matches(self, condition=None):
        """
        retrieve matches data from mongo database
        :return: list of dict
        """
        if condition is None:
            condition = {}

        matches = self.db.find(condition)
        match_data = []
        for match in matches:
            # Remove data where home and away scores are empty strings
            if "" not in [match.get('localteam_score'), match.get('visitorteam_score'), match.get('season')]:
                match.pop("_id")
                match_data.append(match)
            else:
                self.log.warn("match has empty string in home and/or away score: {}".format(match))
        self.log.info("length of data retrieved from matches table: {num}".format(num=len(match_data)))

        return match_data


if __name__ == "__main__":
    de = Matches()
    de.save_matches()
    de.get_all_matches()
