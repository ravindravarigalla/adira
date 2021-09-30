import csv
import json
import os
import queue
import sys
import time

import gevent
import zmq
from locust import TaskSet, task, HttpUser, events, runners, between, SequentialTaskSet
from locust.runners import MasterRunner, WorkerRunner, LocalRunner

os.environ["QUIET_MODE"] = "False"
########################################################
# Reading environment variables and setting up constants

FEEDER_HOST = os.getenv("FEEDER_HOST", "127.0.0.1")
FEEDER_BIND_PORT = os.getenv("FEEDER_BIND_PORT", 5555)
FEEDER_ADDR = f"tcp://{FEEDER_HOST}:{FEEDER_BIND_PORT}"
QUIET_MODE = True if os.getenv("QUIET_MODE", "true").lower() in ['1', 'true', 'yes'] else False
# TASK_DELAY = int(os.getenv("TASK_DELAY", "1000"))

# DATA_SOURCE_PATH = "data/adira_synthetic_domicili_1629699951.csv"
DATA_SOURCE_PATH_DICTIONARY = os.getenv("CONFIG_FILE", "/locust-tasks/data/data.json")


def log(message):
    if not QUIET_MODE:
        print(message)


##################
# Code related to reading csv files
#
class SourceDataReader:
    """
    Handles reading of source data (csv file) and converting it to desired dict form.
    """

    def __init__(self, data_dictionary):

        print("INIT: SourceDataReader")
        self.file_paths, self.end_points = read_data_json(data_dictionary)

    def read(self):

        data_list = []
        for file_path in self.file_paths:
            with open(file_path, "r") as file:
                reader = csv.DictReader(file)

                data = []
                for element in reader:
                    data.append(element)
            data_list.append(data)
        return data_list, self.end_points


def read_data_json(data_dictionary):

    end_points = []
    file_paths = []

    with open(data_dictionary, 'r') as json_data:
        data_dict = json.load(json_data)
        collections = data_dict["locust_data"]

        for collection in collections:
            end_points.append(collection["end_point"])
            file_paths.append(collection["source"])

    return file_paths, end_points

##################
# Code related to receiving messages
#
class ZMQRequester:
    def __init__(self, address="tcp://127.0.0.1:5555"):
        """
        :param address: the addres to connect to; defaults to "tcp://127.0.0.1:5555"
        """

        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect(address)
        log("zmq consumer initialized")

    def await_data(self, end_point):
        # Inform, that you want some data
        self.socket.send_json({"end_points": end_point})

        # wait for the data to arrive and return the message
        return self.socket.recv_json()


##################
# Code related to sending messages
#
class ZMQFeeder:
    def __init__(self, INPUT_DATA_LIST, end_points, address="tcp://127.0.0.1:5555"):
        """
        :param data: list of dict objects
        :param address: indicates interface and port to bind to (see: http://api.zeromq.org/2-1:zmq-tcp#toc6);
                        defaults to "tcp://127.0.0.1:5555"
        """

        self.queue_list = []
        for INPUT_DATA in INPUT_DATA_LIST:
            data_queue = queue.Queue()
            [data_queue.put(i) for i in INPUT_DATA]
            self.queue_list.append(data_queue)

        del INPUT_DATA_LIST
        del INPUT_DATA

        print("Data Queue: ", [data_queue.qsize() for data_queue in self.queue_list])

        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(address)
        log("zmq feeder initialized")

    def run(self):

        while True:
            j = self.socket.recv_json()

            try:
                index = end_points.index(j["end_points"])
                data_queue = self.queue_list[index]
                work = data_queue.get(block=False)

                self.socket.send_json(work)
                data_queue.task_done()

            except ValueError as e:
                log(f"Index of {j['end_point']} could not be found!")

            except queue.Empty:
                # We need to reply something, still...
                log("Queue empty. We need to reply something...")
                self.socket.send_json({})
            # yield
            time.sleep(0)
            print("|", end="")


##################
# Code for detecting run context
#
def is_test_ran_on_master(): return isinstance(runners.locust_runner, MasterRunner)


def is_test_ran_on_slave(): return isinstance(runners.locust_runner, WorkerRunner)


def is_test_ran_on_standalone(): return isinstance(runners.locust_runner, LocalRunner)


def is_locustfile_ran_on_master(): return '--master' in sys.argv


def is_locustfile_ran_on_slave(): return '--worker' in sys.argv


def is_locustfile_ran_on_standalone(): return not ('--worker' in sys.argv or '--master' in sys.argv)


##################
# Code to be run exactly once, at node startup
#

if is_locustfile_ran_on_master() or is_locustfile_ran_on_standalone():
    log("Reading input data...")
    sdr = SourceDataReader(DATA_SOURCE_PATH_DICTIONARY)

    INPUT_DATA_LIST, end_points = sdr.read()
    log(f"Total records read: {[len(INPUT_DATA) for INPUT_DATA in INPUT_DATA_LIST]}")


def init_feeder():
    sender = ZMQFeeder(INPUT_DATA_LIST, end_points, FEEDER_ADDR)
    sender.run()
    print("FEEDER RUN")


##################
# Code to be run before the tests
#

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    log("on_master_start_hatching")
    # start sending the messages (in a separate greenlet, so it doesn't block)
    if is_locustfile_ran_on_master():
        gevent.spawn(init_feeder)


class TestBehaviour(TaskSet):

    def on_start(self):
        self.zmq_consumer = ZMQRequester(FEEDER_ADDR)
        self.orderNo = None
        self.index = 0

        # Taskset cannot receive the endpoints easily. Fetch from json again:
        _, self.end_points = read_data_json(DATA_SOURCE_PATH_DICTIONARY)


    @task
    def task(self):
        log(self.index)

        end_point = self.end_points[self.index]

        self.__logic__(self.orderNo, end_point)

        # Check when full cycle complete and discard persisted orderNo
        if self.index >= len(self.end_points) - 1:
            self.index = 0
            self.orderNo = None

        else:
            self.index += 1

        # gevent.spawn(self.__logic__)

    def __logic__(self, order_no, end_point):
        data = self.zmq_consumer.await_data(end_point)

        if order_no is not None:
            data["orderNo"] = order_no

        log("Received data: " + str(data))

        if data == {}:
            log("Nothing else to do!!!")
        else:
            log(f"using the following data to make a request {data}")

            response = self.client.post(end_point, name=end_point, json=data, verify=False)

            try:
                log(response.content.decode("utf-8"))

                self.orderNo = json.loads(response.content.decode("utf-8"))["orderNo"]
                log("Extracted orderNo: " + self.orderNo)

            except KeyError as k:
                pass
            except json.decoder.JSONDecodeError as j:
                pass


class TestUser(HttpUser):
    """
    Locust user class that uses external data to feed the locust
    """
    tasks = [TestBehaviour]
    wait_time = between(2, 5)


##
def mark(category, message):
    import datetime
    now = datetime.datetime.now()

    new_path = f"./{category}.txt"
    with open(new_path, 'a') as file:
        file.write(f"{now}\t{message}\n")
