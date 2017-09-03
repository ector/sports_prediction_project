import matplotlib.pyplot as plt
import pandas as pd

from te_logger.logger import MyLogger
from tools import SequentialBackwardSelection


# noinspection PyCallByClass
class Plotter(object):
    def __init__(self, x_train, y_train, x_test, y_test, labels=None):
        self.x_train_std = x_train
        self.y_train = y_train
        self.x_test_std = x_test
        self.y_test = y_test
        self.labels = labels
        self.sbs = None
        self.k_features = None
        self.k_best_features = None
        MyLogger.logger(self)

    def use_and_plot_sbs(self, *args):
        fig, ax = plt.subplots()
        # plt.ion()
        scores = []
        k_feat = []
        for key, model in enumerate(args):
            self.sbs = SequentialBackwardSelection(model, k_features=1)
            self.sbs.fit(X=self.x_train_std, y=self.y_train)

            k_feat = [len(k) for k in self.sbs.subsets_]
            scores.append(self.sbs.scores_)

            if self.labels is not None:
                label = self.labels.get(key)
                ax.plot(k_feat, self.sbs.scores_, marker='o', label=label)
            else:
                ax.plot(k_feat, self.sbs.scores_, marker='o')

        max_is = pd.DataFrame(scores, columns=k_feat)
        max_iss = max_is.sum(axis=0).idxmax(axis=0)
        print("column with max is: ", max_iss)

        ax.legend(loc='best', shadow=True)

        plt.ylabel('Accuracy')
        plt.xlabel('Number of features')
        plt.grid()
        plt.show()

    def features_optimiser_index_selector(self, *args):
        scores = []
        k_feat = []
        for key, model in enumerate(args):
            self.sbs = SequentialBackwardSelection(model, k_features=1)
            self.sbs.fit(X=self.x_train_std, y=self.y_train)

            k_feat = [len(k) for k in self.sbs.subsets_]
            scores.append(self.sbs.scores_)

        df = pd.DataFrame(scores, columns=k_feat)
        max_idx = df.sum(axis=0).idxmax(axis=0)
        self.log.info("column with max is: {}".format(max_idx))
        return max_idx

    def get_k_best_features(self, x_data, k_features):
        self.log.info("Selecting k best features from SBS analysis")
        self.k_best_features = list(self.sbs.subsets_[k_features])
        self.log.debug("{} features used are {}".format(len(self.k_best_features),
                                                        x_data.columns[self.k_best_features]))
        self.log.info("Returning k best features")
        return self.k_best_features

    def cross_validate(self, *args):
        for key, model in enumerate(args):
            model.fit(self.x_train_std[:, self.k_best_features], self.y_train)
            self.log.info("Training accuracy {}".format(model.score(self.x_train_std[:, self.k_best_features],
                                                                    self.y_train)))
            self.log.info("Test accuracy {}".format(model.score(self.x_test_std[:, self.k_best_features], self.y_test)))
            y_test_pred = model.predict(self.x_test_std[:, self.k_best_features])
            print(y_test_pred)
