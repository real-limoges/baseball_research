from multiprocessing import JoinableQueue as Queue
from threading import Thread
import threading
from pandas import date_range
import datetime as dt
from time import sleep
import sys
import pymongo

from scrape_mlb import extract_days_games, extract_game_info
from database_info import get_baseball_handles

num_threads = 8

BASE_URL = 'http://gd2.mlb.com/components/game/mlb'


class DownloadWorker(Thread):
    '''
    Thread worker that downloads the information and pushes it into Mongo
    Once activated, it grabs a date from the global date queue. It then
    extracts a day's worth of information from MLBAM, and reports to the
    queue that the task is completed.
    '''

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        '''
        Runs the worker; until the queue is empty, it will get an item
        from the queue, extract the games on that date (unless there were)
        no games that day, and then extract innings and game info are saved
        in a MongoDB.
        '''
        while True:
            date = self.queue.get()
            print date
            
            games, URL = extract_days_games(date.year, date.month, date.day)
            if games is not None:
                for game in games:
                    extract_game_info(URL, str(game), date)
            
            self.queue.task_done()
            sleep(0.5)


if __name__ == '__main__':

    games, innings = get_baseball_handles(database)

    # Determines the dates to look for.
    if games.find().count() == 0 and innings.find().count() == 0:
        start = dt.datetime(2008, 0o1, 0o1)
    else:
        games_min = games.find().sort(
            [('date', pymongo.DESCENDING)]).limit(1)[0]['date']
        innings_min = innings.find().sort(
            [('date', pymongo.DESCENDING)]).limit(1)[0]['date']
        start = min(games_min, innings_min)

    yesterday = dt.datetime.today() - dt.timedelta(days=1)
    dates = date_range(start, yesterday)

    dates_q = Queue()
    [dates_q.put(date) for date in dates]

    workers = []
    for x in range(num_threads):
        worker = DownloadWorker(dates_q)
        workers.append(worker)
        worker.daemon = True
        worker.start()
    dates_q.join()
