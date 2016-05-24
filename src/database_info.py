from pymongo import MongoClient


def get_baseball_handles():
    client = MongoClient()
    db = client[database]

    return (db['games'], db['innings'])
