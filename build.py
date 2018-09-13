#!/usr/local/bin/python3

import sys
from itertools import chain

import time
import datetime
import subprocess as sp
from subprocess import PIPE
import threading


def print_output(cmd, terminal_output):
    stdout = terminal_output.stdout
    stderr = terminal_output.stderr

    if stdout is not None and len(stdout) > 0:
        print("{} output:\n{}".format(cmd, stdout.decode("utf-8").strip()))
    if stderr is not None and len(stderr) > 0:
        print("{} error:\n{}".format(cmd, stderr.decode("utf-8").strip()))


def execute_with_python(command):
    if command is not None:
        cmd = command.split(" ")[1]
        completed = sp.run(command, shell=True, stdout=PIPE, stderr=PIPE)
        # print_output(cmd=cmd, terminal_output=completed)


class Spinner:
    busy = False
    delay = 0.1

    @staticmethod
    def spinning_cursor():
        while 1:
            for cursor in '|/-\\': yield cursor

    def __init__(self, delay=None):
        self.spinner_generator = self.spinning_cursor()
        if delay and float(delay): self.delay = delay

    def spinner_task(self):
        while self.busy:
            sys.stdout.write(next(self.spinner_generator))
            sys.stdout.flush()
            time.sleep(self.delay)
            sys.stdout.write('\b')
            sys.stdout.flush()

    def start(self):
        self.busy = True
        threading.Thread(target=self.spinner_task).start()

    def stop(self):
        self.busy = False
        time.sleep(self.delay)


def print_with_spinner_when_running_py_file(filename):
    spinner = Spinner()
    print("Running {}.py".format(filename))
    spinner.start()
    execute_with_python(command="python3 tools/{}.py".format(filename))
    spinner.stop()


def set_all_scripts_on_fire():
    """
    run all scripts
    :return:
    """
    start = time.time()

    time_now = datetime.datetime.now()

    # Get gmaes fixture from the web
    if time_now.hour in chain(range(4, 12), range(16, 18)):

        print_with_spinner_when_running_py_file(filename="pull_data/download_fixtures")
        # pass

    # Pull dated result and get next 3 days fixtures ready for prediction
    if time_now.hour in range(4, 21):

        print_with_spinner_when_running_py_file(filename="pull_data/pull_data")
        print_with_spinner_when_running_py_file(filename="fixtures")

    print_with_spinner_when_running_py_file(filename="process_data/process_previous_data")

    print_with_spinner_when_running_py_file(filename="build_model/train_wdw_model")

    print_with_spinner_when_running_py_file(filename="build_model/train_over_under_25_model")

    print_with_spinner_when_running_py_file(filename="predictors/match_predictor")

    print("The whole program took: {} sec".format(time.time() - start))


if __name__ == '__main__':
    set_all_scripts_on_fire()
