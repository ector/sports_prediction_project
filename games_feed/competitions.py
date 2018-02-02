from te_logger.logger import log
from tools.utils import get_data_from_football_api_com, get_config
from tools.db_util.mongo_cmd import MongoConnect


class Competitions(object):
    """
    Retrieve, store and get all competitions provided by football-api.com
    """
    def __init__(self):
        self.log = log
        self.comp_url = "http://api.football-api.com/2.0/competitions?Authorization={auth}"
        self.db = MongoConnect().competition_db()
        self.auth = get_config("db").get("auth_key")

    def save_competitions(self):
        """
        Store competitions info into database
        :return:
        """
        self.comp_url = self.comp_url.format(auth=self.auth)

        comps = get_data_from_football_api_com(url=self.comp_url)
        if comps is not None:
            new_comps = []
            for comp in comps:
                if self.db.find({"id": comp.get("id")}).count() == 0:
                    new_comps.append(comp)
            if len(new_comps) != 0:
                result = self.db.insert_many(new_comps)
                log.info("inserted data ids: {id}".format(id=result.inserted_ids))
            else:
                log.info("no new data to store")

    def get_all_competitions(self):
        """
        retrieve competition data from mongo database
        :return:
        """
        comps = self.db.find({})
        comp_data = []
        for comp in comps:
            comp.pop("_id")
            print("Data is: {}".format(comp))
            comp_data.append(comp)
        self.log.info("length of data retrieved from competition table: {num}".format(num=len(comp_data)))
        return comp_data


if __name__ == "__main__":
    de = Competitions()
    # de.save_competitions()
    de.get_all_competitions()
