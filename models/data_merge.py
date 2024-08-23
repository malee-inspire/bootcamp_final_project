import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from routes.snow_conn import snow_create_connection
from COVID_SAMPLE_DB_setup.database_setup import create_table

import json

#### open and read json data
f = open('../datasets/global.json', 'r')

data = json.load(f)

glob_array = []

data_filter_list = ['countryName','wbCountryCode','m49code','iso3166Alpha2Code','wbRegion','wbRegionCode','wbIncomeLevelCode',
             'wbIncomeLevelName','wbPopulation2019','owidHasVaccine','mostRecentResponseDate','mostRecentExtendedBreakCode',
             'mostRecentEducationStatusCode', 'mostRecentEducationStatusPrePrimaryCode', 'mostRecentVaccineAvailabilityForTeachersCode']

for _ in data:
    glob_data = {}
    for ele in _['fields']:
        if ele in data_filter_list:
            glob_data[ele] = (_['fields'][ele] if _['fields'][ele] is not None else 0) or (0 if numpy.isnan(_['fields'][ele]) else _['fields'][ele])
    glob_array.append(glob_data)

df = pd.DataFrame(glob_array)
print(df.info)

#### read csv file
# filepath = '../datasets/owid-covid-data.csv'
# if not os.path.exists(filepath):
#     print(f"File not found: {filepath}")
# else:
#     df = pd.read_csv(filepath)

# Identify missing data
missing_data_summary = df.isnull().sum()
print("Missing data summary:\n", missing_data_summary)

df = df.dropna()
df = df.dropna(axis=1)
print(df.isnull().sum())

#### snowflake python connector to load data into snoflake
def main():
    ### Establish a connection to Snowflake
    conn = snow_create_connection()

    cur = conn.cursor()
    try:
        # create_table(cur)

        cur.execute('''
            CREATE TABLE IF NOT EXISTS "globalData" (
                "countryName" VARCHAR(70),
                "wbCountryCode" VARCHAR(20),
                "m49code" FLOAT,
                "iso3166Alpha2Code" VARCHAR(4),
                "wbRegion" VARCHAR,
                "wbRegionCode" VARCHAR,
                "wbIncomeLevelCode" VARCHAR,
                "wbIncomeLevelName" VARCHAR,
                "wbPopulation2019" FLOAT,
                "owidHasVaccine" VARCHAR,
                "mostRecentResponseDate" DATE,
                "mostRecentExtendedBreakCode" INT,  -- Fixed: removed the (2)
                "mostRecentEducationStatusCode" VARCHAR,
                "mostRecentEducationStatusPrePrimaryCode" VARCHAR,
                "mostRecentVaccineAvailabilityForTeachersCode" VARCHAR
            );
        ''')

    except Exception as e:
        print(f"table creation error : {e}")

    ### commit the transaction
    conn.commit()
    # Reset the index to convert the existing index to a column
    df.reset_index(drop=True, inplace=True)

    ###write the DataFrame to Snowflake
    write_pandas(conn, df, table_name="globalData")

    ### close the connection after load data
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()