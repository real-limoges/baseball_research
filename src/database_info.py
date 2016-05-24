from pymongo import MongoClient


def get_baseball_handles(database):
    client = MongoClient()
    db = client[database]

    return [db['games'], db['innings']]
