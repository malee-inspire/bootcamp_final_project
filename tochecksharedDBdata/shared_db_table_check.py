from routes.snow_conn import snow_create_connection

### Establish a connection to Snowflake
conn = snow_create_connection()

# Create a cursor object
cur = conn.cursor()

# Step 1: Get the list of tables
cur.execute("SHOW TABLES IN SCHEMA COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC;")
tables = cur.fetchall()

# Step 2: Loop through each table and execute the SELECT query
for table in tables:
    table_name = table[1]  # table_name is typically the second column in SHOW TABLES output
    query = f"SELECT * FROM {table_name} LIMIT 10;"
    cur.execute(query)

    # Fetch and print the results (you can modify this as needed)
    results = cur.fetchall()
    print(f"Results for table {table_name}:")
    for row in results:
        print(row)

# Close the cursor and connection
cur.close()
conn.close()
