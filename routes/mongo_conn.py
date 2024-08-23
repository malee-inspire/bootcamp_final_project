from pymongo import MongoClient

def get_mongo_connection():
    # MongoDB connection setup
    try:
        client = MongoClient('mongodb://localhost:27017/')
        mongo_db = client['mongodb_userdbd']
    except Exception as e:
        print(f'mongoDb does not create successfully: {e}')

    return mongo_db