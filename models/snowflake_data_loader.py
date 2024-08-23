import snowflake.connector
from routes.snow_conn import snow_create_connection

# Define your connection details
conn = snow_create_connection()

# Define tables and the columns to check for NULLs
tables = {
    "APPLE_MOBILITY": ["COUNTRY_REGION", "PROVINCE_STATE", "DATE", "DIFFERENCE", "LAST_UPDATED_DATE"],
    "CDC_INPATIENT_BEDS_ALL": ["STATE", "DATE", "INPATIENT_BEDS_IN_USE_PCT", "INPATIENT_BEDS_IN_USE_PCT_LOWER_BOUND",
                               "INPATIENT_BEDS_IN_USE_PCT_UPPER_BOUND", "INPATIENT_BEDS_LOWER_BOUND",
                               "INPATIENT_BEDS_OCCUPIED", "INPATIENT_BEDS_UPPER_BOUND",
                               "TOTAL_INPATIENT_BEDS", "TOTAL_INPATIENT_BEDS_LOWER_BOUND",
                               "TOTAL_INPATIENT_BEDS_UPPER_BOUND"],
    "CDC_INPATIENT_BEDS_COVID_19": ["STATE", "DATE", "INPATIENT_BEDS_IN_USE_PCT",
                                    "INPATIENT_BEDS_IN_USE_PCT_LOWER_BOUND",
                                    "INPATIENT_BEDS_IN_USE_PCT_UPPER_BOUND", "INPATIENT_BEDS_LOWER_BOUND",
                                    "INPATIENT_BEDS_OCCUPIED", "INPATIENT_BEDS_UPPER_BOUND",
                                    "TOTAL_INPATIENT_BEDS", "TOTAL_INPATIENT_BEDS_LOWER_BOUND",
                                    "TOTAL_INPATIENT_BEDS_UPPER_BOUND"],
    "CDC_INPATIENT_BEDS_ICU_ALL": ["DATE", "STAFFED_ADULT_ICU_BEDS_OCCUPIED", "STAFFED_ADULT_ICU_BEDS_OCCUPIED_LOWER_BOUND",
                                   "STAFFED_ADULT_ICU_BEDS_OCCUPIED_PCT", "STAFFED_ADULT_ICU_BEDS_OCCUPIED_PCT_LOWER_BOUND",
                                   "STAFFED_ADULT_ICU_BEDS_OCCUPIED_PCT_UPPER_BOUND", "STAFFED_ADULT_ICU_BEDS_OCCUPIED_UPPER_BOUND",
                                   "TOTAL_STAFFED_ICU_BEDS", "TOTAL_STAFFED_ICU_BEDS_LOWER_BOUND",
                                   "TOTAL_STAFFED_ICU_BEDS_UPPER_BOUND"],
    "CDC_POLICY_MEASURES": ["STATE_ID", "DATE", "LAST_UPDATE_DATE", "POLICY_LEVEL", "POLICY_TYPE"],
    "CDC_REPORTED_PATIENT_IMPACT": ["STATE", "DATE", "LAST_UPDATE_DATE", "INPATIENT_BEDS", "INPATIENT_BEDS_COVERAGE",
                                    "INPATIENT_BEDS_USED", "INPATIENT_BEDS_USED_COVERAGE", "INPATIENT_BEDS_USED_COVID",
                                    "INPATIENT_BEDS_USED_COVID_COVERAGE", "TOTAL_STAFFED_ADULT_ICU_BEDS"],
    "CDC_TESTING": ["DATE", "ISO3166_1", "POSITIVE", "NEGATIVE"],
    "CT_US_COVID_TESTS": ["COUNTRY_REGION", "PROVINCE_STATE", "DATE", "LAST_UPDATED_DATE", "DEATH",
                          "DEATH_SINCE_PREVIOUS_DAY", "HOSPITALIZED", "HOSPITALIZEDCUMULATIVE",
                          "HOSPITALIZEDCUMULATIVEINCREASE", "HOSPITALIZEDCURRENTLY", "HOSPITALIZEDCURRENTLYINCREASE",
                          "HOSPITALIZED_SINCE_PREVIOUS_DAY", "POSITIVE", "NEGATIVE", "PENDING"],
    "DATABANK_DEMOGRAPHICS": ["COUNTRY_REGION", "COUNTY", "STATE", "LATITUDE", "LONGITUDE",
                              "TOTAL_FEMALE_POPULATION", "TOTAL_MALE_POPULATION", "TOTAL_POPULATION"],
    "DEMOGRAPHICS": ["COUNTY", "LATITUDE", "LONGITUDE", "STATE", "TOTAL_FEMALE_POPULATION",
                     "TOTAL_MALE_POPULATION", "TOTAL_POPULATION"],
    "ECDC_GLOBAL": ["COUNTRY_REGION", "DATE", "LAST_UPDATE_DATE", "CASES", "CASES_SINCE_PREV_DAY", "DEATHS", "POPULATION"],
    "ECDC_GLOBAL_WEEKLY": ["COUNTRY_REGION", "DATE", "LAST_UPDATE_DATE", "CASES_WEEKLY", "DEATHS_WEEKLY", "POPULATION"],
    "GOOG_GLOBAL_MOBILITY_REPORT": ["COUNTRY_REGION", "PROVINCE_STATE", "SUB_REGION_2", "DATE", "LAST_UPDATE_DATE",
                                    "GROCERY_AND_PHARMACY_CHANGE_PERC", "RESIDENTIAL_CHANGE_PERC", "TRANSIT_STATIONS_CHANGE_PERC",
                                    "WORKPLACES_CHANGE_PERC"],
    "HDX_ACAPS": ["COUNTRY_STATE", "REGION", "DATE_IMPLEMENTED", "ENTRY_DATE", "LAST_UPDATED_DATE"],
    "HUM_RESTRICTIONS_AIRLINE": ["COUNTRY", "LAT", "LONG", "PUBLISHED", "LAST_UPDATE_DATE"],
    "HUM_RESTRICTIONS_COUNTRY": ["COUNTRY", "LAT", "LONG", "INFO_DATE", "LAST_UPDATE_DATE", "PUBLISHED"],
    "IHME_COVID_19": ["COUNTRY_REGION", "PROVINCE_STATE", "DATE", "LAST_UPDATED_DATE", "DEATHS_MEAN", "ALLBED_MEAN",
                      "BEDOVER_MEAN", "ICUBED_MEAN", "ICUOVER_MEAN", "INVVEN_MEAN", "NEWICU_MEAN", "TOTDEA_MEAN"],
    "JHU_COVID_19": ["COUNTRY_REGION", "COUNTY", "PROVINCE_STATE", "LONG", "LAT", "DATE", "LAST_UPDATED_DATE", "DIFFERENCE"],
    "JHU_COVID_19_TIMESERIES": ["COUNTRY_REGION", "COUNTY", "PROVINCE_STATE", "LONG", "LAT", "DATE", "LAST_UPDATE_DATE", "DIFFERENCE"],
    "JHU_DASHBOARD_COVID_19_GLOBAL": ["COUNTRY_REGION", "COUNTY", "LAT", "LONG", "PROVINCE_STATE", "DATE", "LAST_UPDATE_DATE",
                                      "ACTIVE", "CONFIRMED", "DEATHS", "HOSPITALIZATION_RATE", "INCIDENT_RATE", "MORTALITY_RATE",
                                      "PEOPLE_TESTED", "RECOVERED", "TESTING_RATE"],
    "JHU_VACCINES": ["COUNTRY_REGION", "PROVINCE_STATE", "DATE", "LAST_UPDATE_DATE", "DOSES_ADMIN_TOTAL", "DOSES_ALLOC_TOTAL",
                     "DOSES_SHIPPED_TOTAL", "PEOPLE_TOTAL_2ND_DOSE", "PEOPLE_TOTAL"],
    "NYT_US_COVID19": ["COUNTY", "STATE", "DATE", "LAST_UPDATE_DATE", "DEATHS", "CASES"],
    "NYC_HEALTH_TESTS": ["COUNTRY_REGION", "DATE", "LAST_UPDATED_DATE", "TOTAL_COVID_TESTS", "PERCENT_POSITIVE", "COVID_CASE_COUNT"],
    "NYT_US_REOPEN_STATUS": ["STATE_CODE", "STATE", "RESTRICTION_START", "RESTRICTION_END", "LAST_UPDATE_DATE", "POPULATION"],
    "OWID_VACCINATIONS": ["COUNTRY_REGION", "DATE", "LAST_OBSERVATION_DATE", "LAST_UPDATE_DATE", "VACCINES",
                          "TOTAL_VACCINATIONS_PER_HUNDRED", "TOTAL_VACCINATIONS", "PEOPLE_VACCINATED_PER_HUNDRED",
                          "PEOPLE_VACCINATED", "PEOPLE_FULLY_VACCINATED_PER_HUNDRED", "PEOPLE_FULLY_VACCINATED",
                          "DAILY_VACCINATIONS_PER_MILLION", "DAILY_VACCINATIONS"],
    "PCM_DPS_COVID19": ["COUNTRY_REGION", "LAT", "LONG", "PROVINCE_STATE", "DATE", "LAST_UPDATED_DATE", "CASES", "CASE_TYPE", "DIFFERENCE"],
    "WHO_DAILY_REPORT": ["COUNTRY_REGION", "ISO3166_1", "DATE", "CASES", "CASES_TOTAL", "CASES_TOTAL_PER_100000",
                         "DEATHS", "DEATHS_TOTAL", "DEATHS_TOTAL_PER_100000"],
    "SCS_BE_DETAILED_PROVINCE_CASE_COUNTS": ["PROVINCE", "REGION", "DATE", "LAST_UPDATED_DATE", "TOTAL_CASES", "NEW_CASES", "AGEGROUP", "SEX"],
    "SCS_BE_DETAILED_MORTALITY": ["REGION", "DATE", "LAST_UPDATED_DATE", "DEATHS", "AGEGROUP", "SEX"],
    "SCS_BE_DETAILED_HOSPITALISATIONS": ["REGION", "PROVINCE", "DATE", "LAST_UPDATED_DATE", "NEW_IN", "NEW_OUT", "TOTAL_IN",
                                         "TOTAL_IN_ECMO", "TOTAL_IN_ICU", "TOTAL_IN_RESP"],
    "SCS_BE_DETAILED_TESTS": ["DATE", "LAST_UPDATED_DATE", "TESTS"],
    "VH_CAN_DETAILED": ["PROVINCE_STATE", "ISO3166_1", "DATE", "LAST_UPDATED_DATE", "CASES", "DEATHS", "HEALTHCARE_REGION"],
    "HS_BULK_DATA": ["BEDS", "HEALTHCARE_PROVIDER_TYPE", "NAME", "OPERATOR"]
}


# Function to generate SQL for each table
def generate_sql(table_name, columns):
    null_checks = " AND ".join([f"{col} IS NOT NULL" for col in columns])
    sql = f"""
    CREATE OR REPLACE TABLE merged_insights.{table_name.lower()} AS
    SELECT *
    FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.{table_name}
    WHERE {null_checks};
    """
    return sql


# Main function to connect to Snowflake and execute the queries
def main():
    # conn = snow_create_connection()
    cur = conn.cursor()

    try:
        for table_name, columns in tables.items():
            sql_query = generate_sql(table_name, columns)
            print(f"Executing query for table: {table_name}")
            cur.execute(sql_query)

        print("All tables processed successfully!")
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
