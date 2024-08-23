def create_table(cur):
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
            "mostRecentExtendedBreakCode" INT(2),
            "mostRecentEducationStatusCode" VARCHAR,
            "mostRecentEducationStatusPrePrimaryCode" VARCHAR,
            "mostRecentVaccineAvailabilityForTeachersCode" VARCHAR
        );
    ''')