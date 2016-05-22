'''
This script originally populates the database. Once run,
scheduled_scrape_mlb.py should scheduled to run every day.
'''

import urllib2
from bs4 import BeautifulSoup
import requests
import datetime as dt
from time import sleep
import json

BASE_URL = 'http://gd2.mlb.com/components/game/mlb'
today = dt.datetime.today()


def generate_dates():
    years = xrange(2016, 2017)
    months = xrange(1, 13)
    days = xrange(1, 32)

    return years, months, days


def extract_games(year, month, day):
    try:
        s_year = '/year_{0:04d}'.format(year)
        s_mon = '/month_{0:02d}'.format(month)
        s_day = '/day_{0:02d}'.format(day)

        URL = BASE_URL + s_year + s_mon + s_day
        print URL
        req = urllib2.Request(URL)
        sleep(1)
        res = urllib2.urlopen(req)
        soup = BeautifulSoup(res.read(), 'html.parser')

        return [x.contents[0] for x in soup.find_all(
            'a') if x.contents[0].startswith(' gid')], URL

    except:
        print "{}/{}/{} was not found".format(year, month, day)
        return None


def extract_game_info():
    pass


if __name__ == '__main__':

    years, months, days = generate_dates()

    for year in years:
        for month in months:
            for day in days:
                games, URL = extract_games(year, month, day)
                if games is not None:
                    for game in games:
                        extract_game_info(URL, game)

    # except URLError as e:
    #     print e.reason
