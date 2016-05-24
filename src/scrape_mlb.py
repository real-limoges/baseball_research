'''
This script originally populates the database. Once run,
scheduled_scrape_mlb.py should scheduled to run every day.
'''

from bs4 import BeautifulSoup
import urllib2

import datetime as dt
from pandas import date_range
from time import sleep

import pymongo
from pymongo import MongoClient
import xmltodict as xtod

import logging

# Global Variables
BASE_URL = 'http://gd2.mlb.com/components/game/mlb'
logging.basicConfig(filename='../logs/logging.log')

from database_info import get_baseball_handles

games, innings = get_baseball_handles()


def pprint_log_suc(msg):
    '''
    INPUT : Logger Object, String (Message)
    OUTPUT : None

    Outputs a success message to the appropriate logger.
    '''
    level = 20
    logging.log(level=level, msg=msg)


def pprint_log_ex(ex, level, opt_string=''):
    '''
    INPUT : Exception Object, Logger Object, Int (Level),
            String (Optional Extra)
    OUTPUT : None

    Outputs an error message to the appropriate logger.
    '''

    template = "An exception of type {0} occured. Arguments:\n{1!r}"
    msg = template.format(type(ex).__name__, ex.args)
    msg += " " + opt_string
    logging.log(level=level, msg=msg)


def extract_days_games(year, month, day):
    '''
    INPUT : Int (Year), Int (Month), Int (Day)
    OUTPUT : Return List of GameIDs

    Creates two URLs to query. If the page doesn't exist (404) then
    it logs it as not existing (for instance December 30, 2015).
    If it exists, it pushes it into a MongoDB.
    '''
    try:
        s_year = '/year_{0:04d}'.format(year)
        s_mon = '/month_{0:02d}'.format(month)
        s_day = '/day_{0:02d}'.format(day)

        URL = BASE_URL + s_year + s_mon + s_day
        req = urllib2.Request(URL)
        res = urllib2.urlopen(req)
        sleep(0.2)

        soup = BeautifulSoup(res.read(), 'html.parser')

        msg = "Successfully Extracted {}/{}/{} Links".format(year, month, day)
        pprint_log_suc(msg)

        return [x.contents[0] for x in soup.find_all(
            'a') if x.contents[0].startswith(' gid')], URL

    except Exception as e:

        opt_msg = "{}/{}/{} was not found".format(year, month, day)
        pprint_log_ex(e, level=30, opt_string=opt_msg)

        return None, None

def extract_info(URL, col, key, date):
    last_date = col.find().sort([('date', pymongo.DESCENDING)])
    if last_date.count() == 0 or date > last_date.limit(1)[0]['date']:
        try:
            req= urllib2.Request(URL)
            res = urllib2.urlopen(req)
            sleep(0.2)

            data = xtod.parse(res.read())[key]
            data['date'] = date

            col.insert_one(data)

            msg = "Sucesfully entered {}".format(key)
            pprint_log_suc(msg)

            return None

        except urllib2.HTTPError as e:
            # 404 means no games were played that day, and its in the
            # off season
            if e.code == 404:
                msg = "on " + URL[-25:]
                pprint_log_ex(e, level=30)
                return None, None

            #Other Errors are more important to be logged
            else:
                msg = " Error Code : {}".format(e.code)
                pprint_log_ex(e, level=40, opt_string=msg)
                
                sleep(5.)
                extract_game_info(URL, game)
                errors.append(URL)

        # Critical Exception - Connection Error, etc...
        except Exception as ex:
            msg = "on " + URL[-25:]
            pprint_log_ex(ex, level=50, opt_string=msg)

            return None

def extract_game_info(URL, game, date):
    '''
    INPUT : String (BASE_URL), String (Game), DateTime

    Extract game and inning_all information on each game. Convert from
    XML to python dictionary. Python dictionaries are supported to be
    inserted into MongoDB. Pushes game info and inning info into Mongo.
    '''

    updated_URL_game_info = URL + "/" + game.strip() + '/game.xml'
    updated_URL_innings = URL + "/" + game.strip() + '/inning/inning_all.xml'

    last_date_g = games.find().sort([('date', pymongo.DESCENDING)])
    last_date_i = innings.find().sort([('date': pymongo.DESCENDING)]) 

    if date > last_date_g:
        extract_info(updated_URL_game_info, games, 'game', date)
    
    if date > last_date_i:
        extract_info(updated_URL_innings, innings, 'game', date)



