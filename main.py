from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient
import json
from routes.snow_conn import snow_create_connection
from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional
from bson import ObjectId
import snowflake.connector
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


try:
    # Your existing code
    app = FastAPI()
    # ... rest of the code ...
except Exception as e:
    print(f"An error occurred: {e}")

# Database Connections
### Establish a connection to snowflake
try:
    conn = snow_create_connection()
    print("Connection was successful!")
except snowflake.connector.errors.ProgrammingError as e:
    logger.error(f"Snowflake Programming Error: {e}")
    raise HTTPException(status_code=500, detail="Snowflake Programming Error")
except Exception as e:
    logger.error(f"Unexpected Error: {e}")
    raise HTTPException(status_code=500, detail="Internal Server Error")

# MongoDB connection setup
try:
    client = MongoClient('mongodb://localhost:27017/')
    mongo_db = client['mongodb_userdbd']
except Exception as e:
    print(f'mongoDb does not create successfully: {e}')

# Define MongoDB collections
comments_collection = mongo_db["comments"]
annotations_collection = mongo_db["annotations"]

# Define Pydantic models for requests
class Comment(BaseModel):
    user_id: str
    data_point_id: str
    comment_text: str

class Annotation(BaseModel):
    user_id: str
    visualization_id: str
    data_point_id: str
    annotation_text: str
    location_data: dict
    filter_criteria: dict

#### Query Snowflake Data
@app.get("/query_data/")
async def query_data(table: str, country: Optional[str] = None, state: Optional[str] = None,
                     date: Optional[str] = None):
    query = f'SELECT * FROM COVID_SAMPLE_DB.public."{table}" WHERE 1=1'

    if country:
        query += f" AND COUNTRY_REGION = '{country}'"
    if state:
        query += f" AND PROVINCE_STATE = '{state}'"
    if date:
        query += f" AND DATE = '{date}'"

    logger.info(f"Executing query: {query}")

    cursor = conn.cursor()
    cursor.execute(query)

    result = cursor.fetchall()
    logger.info(f"Executing result: {result}")
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    data = [dict(zip(columns, row)) for row in result]
    return {"data": data}


### Post Comments to MongoDB
@app.post("/comments/")
async def post_comment(comment: Comment):
    new_comment = {
        "user_id": comment.user_id,
        "data_point_id": comment.data_point_id,
        "comment_text": comment.comment_text,
        "created_at": datetime.now()
    }
    comment_id = comments_collection.insert_one(new_comment).inserted_id
    return {"message": "Comment added successfully", "comment_id": str(comment_id)}


### Post Annotations to MongoDB
@app.post("/annotations/")
async def post_annotation(annotation: Annotation):
    new_annotation = {
        "user_id": annotation.user_id,
        "visualization_id": annotation.visualization_id,
        "data_point_id": annotation.data_point_id,
        "annotation_text": annotation.annotation_text,
        "location_data": annotation.location_data,
        "filter_criteria": annotation.filter_criteria,
        "timestamp": datetime.now()
    }
    annotation_id = annotations_collection.insert_one(new_annotation).inserted_id
    return {"message": "Annotation added successfully", "annotation_id": str(annotation_id)}

##### On-the-Fly Data Processing
##calculating the average percentage of inpatient beds in use across states
@app.get("/inpatient_beds_average/")
async def inpatient_beds_average(date: str):
    query = f"""
        SELECT AVG(INPATIENT_BEDS_IN_USE_PCT)
        FROM CDC_INPATIENT_BEDS_ALL
        WHERE DATE = '{date}'
    """

    cursor = conn.cursor()
    cursor.execute(query)
    avg_pct = cursor.fetchone()[0]
    cursor.close()

    return {"date": date, "average_inpatient_beds_in_use_pct": avg_pct}
