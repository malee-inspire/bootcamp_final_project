import snowflake.connector

##### snowflake python connector to load data into snoflake
def snow_create_connection():
    conn = snowflake.connector.connect(
        user="",
        password="",
        account="",
        region="",
        host="",
        warehouse="COMPUTE_WH",
        database="COVID_SAMPLE_DB",
        schema="MERGED_INSIGHTS",
        role="ACCOUNTADMIN"
    )
    return conn