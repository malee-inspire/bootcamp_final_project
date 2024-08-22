from pymongo import MongoClient
from marshmallow import Schema, fields, ValidationError, pre_load
from bson.objectid import ObjectId
from datetime import datetime, timezone

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['covid_analysis_db']

# Define the Comments schema
class CommentSchema(Schema):
    comment_id = fields.String(required=True)
    user_id = fields.String(required=True)
    data_point_id = fields.String(required=True)
    comment_text = fields.String(required=True)
    timestamp = fields.DateTime(required=True)

    @pre_load
    def set_defaults(self, data, **kwargs):
        if 'comment_id' not in data:
            data['comment_id'] = str(ObjectId())
        if 'timestamp' not in data:
            # Set the timestamp to a timezone-aware UTC datetime in ISO 8601 format
            data['timestamp'] = datetime.now(timezone.utc).isoformat()
        return data

# Define the Annotations schema
class AnnotationSchema(Schema):
    annotation_id = fields.String(required=True)
    user_id = fields.String(required=True)
    data_point_id = fields.String(required=True) ##
    annotation_text = fields.String(required=True)
    timestamp = fields.DateTime(required=True)

    visualization_id = fields.String(required=True)

    @pre_load
    def set_defaults(self, data, **kwargs):
        if 'annotation_id' not in data:
            data['annotation_id'] = str(ObjectId())
        if 'timestamp' not in data:
            # Set the timestamp to a timezone-aware UTC datetime in ISO 8601 format
            data['timestamp'] = datetime.now(timezone.utc).isoformat()
        return data

# Define the External Sources schema
class ExternalSourceSchema(Schema):
    source_id = fields.String(required=True)
    data_point_id = fields.String(required=True)
    source_url = fields.URL(required=True)
    description = fields.String(required=True)

    @pre_load
    def set_defaults(self, data, **kwargs):
        if 'source_id' not in data:
            data['source_id'] = str(ObjectId())
        return data

# Instantiate schemas
comment_schema = CommentSchema()
annotation_schema = AnnotationSchema()
external_source_schema = ExternalSourceSchema()

# Example: Insert a new comment
def insert_comment(data):
    try:
        # Validate data against the schema
        validated_data = comment_schema.load(data)
        # Insert into MongoDB
        db.comments.insert_one(validated_data)
        print("Comment inserted successfully!")
    except ValidationError as err:
        print("Validation Error:", err.messages)

# Example: Insert a new annotation
def insert_annotation(data):
    try:
        # Validate data against the schema
        validated_data = annotation_schema.load(data)
        # Insert into MongoDB
        db.annotations.insert_one(validated_data)
        print("Annotation inserted successfully!")
    except ValidationError as err:
        print("Validation Error:", err.messages)

# Example: Insert a new external source
def insert_external_source(data):
    try:
        # Validate data against the schema
        validated_data = external_source_schema.load(data)
        # Insert into MongoDB
        db.external_sources.insert_one(validated_data)
        print("External source inserted successfully!")
    except ValidationError as err:
        print("Validation Error:", err.messages)

# Example usage
new_comment = {
    "user_id": str(ObjectId()),
    "data_point_id": "data_point_123",
    "comment_text": "This is an insightful comment!"
}

insert_comment(new_comment)

new_annotation = {
    "user_id": str(ObjectId()),
    "visualization_id": "time_series_01",
    "data_point_id": "data_point_123",
    "timestamp": datetime.now(),
    "annotation_text": "Significant drop in cases after policy implementation.",
    "location_data": {"country": "USA", "region": "California", "latitude": 36.7783, "longitude": -119.4179},
    "filter_criteria": {"date_range": {"start_date": "2020-05-01", "end_date": "2020-06-01"}}
}

insert_annotation(new_annotation)

new_external_source = {
    "data_point_id": "data_point_123",
    "source_url": "https://example.com",
    "description": "This is an external source."
}

insert_external_source(new_external_source)
