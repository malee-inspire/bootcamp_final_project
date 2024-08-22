from pymongo import MongoClient

def get_mongo_connection():
    return MongoClient("mongodb://localhost:27017/")