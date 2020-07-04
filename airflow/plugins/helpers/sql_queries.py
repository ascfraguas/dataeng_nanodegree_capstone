class SqlQueries:
    
    create_schemas_query = """
    CREATE SCHEMA IF NOT EXISTS immigration;
    CREATE SCHEMA IF NOT EXISTS temperature;
    CREATE SCHEMA IF NOT EXISTS outputs;
    COMMIT;
    """
    
    create_tables_query = """
    CREATE TABLE IF NOT EXISTS immigration.us_entries (
        admnum bigint NOT NULL,
        i94bir double precision,
        gender varchar(1),
        i94visa varchar,
        i94cit varchar,
        i94res varchar,
        i94addr varchar,
        i94mode varchar,
        arrival_day bigint,
        arrival_month bigint,
        arrival_year bigint,
        departure_day bigint,
        departure_month bigint,
        departure_year bigint,
        length_of_stay bigint,
        CONSTRAINT us_entries_pkey PRIMARY KEY (admnum))
    SORTKEY(arrival_year, arrival_month)
    ;
    CREATE TABLE IF NOT EXISTS immigration.country_codes (
        code varchar,
        country_name varchar
        )
    ;
    CREATE TABLE IF NOT EXISTS immigration.port_codes (
        code varchar,
        port_name varchar
        )
    ;
    CREATE TABLE IF NOT EXISTS immigration.entry_channel_codes (
        code varchar,
        entry_channel varchar
        )
    ;
    CREATE TABLE IF NOT EXISTS immigration.state_codes (
        code varchar,
        state_name varchar
        )
    ;
    CREATE TABLE IF NOT EXISTS immigration.trip_reason_codes (
        code varchar,
        trip_reason varchar
        )
    ;
    CREATE TABLE IF NOT EXISTS temperature.full_temperature_data (
        dt varchar,
        averagetemperature double precision,
        averagetemperatureuncertainty double precision,
        city varchar,
        country varchar,
        latitude varchar,
        longitude varchar)
    SORTKEY(country)
    ;
    COMMIT;
    """
    
    copy_immigration_data = """
    COPY immigration.us_entries FROM '{}' IAM_ROLE '{}' FORMAT AS PARQUET;
    COMMIT;
    """
    
    copy_temperature_data = """
    TRUNCATE TABLE temperature.full_temperature_data;
    COPY temperature.full_temperature_data FROM '{}' IGNOREHEADER AS 1 DELIMITER ',' IAM_ROLE '{}';
    COMMIT;
    """
    
    run_temps_summary = """
    DROP TABLE IF EXISTS temperature.temp_summary;
    CREATE TABLE temperature.temp_summary AS (
        SELECT UPPER(country) as country_name, AVG(averagetemperature) as mean_temp, STDDEV_POP(averagetemperature) as stddev_temp 
        FROM temperature.full_temperature_data 
        GROUP BY country);
    COMMIT;
    """
    
    demographics_by_channel = """
    CREATE TABLE outputs.{}{}_demographics_by_channel AS (
        SELECT codes.entry_channel, data.gender, data.average_age FROM (
            (SELECT CAST(CAST(i94mode AS DOUBLE PRECISION) AS INT) as code, gender, AVG(i94bir) as average_age
            FROM immigration.us_entries WHERE arrival_month={} and arrival_year={} and i94bir>0 and i94mode!='nan'
            GROUP BY i94mode, gender) AS data
            LEFT JOIN immigration.entry_channel_codes AS codes
            ON codes.code = data.code));
    COMMIT;
    """
    
    length_of_stay = """
    CREATE TABLE outputs.{}{}_length_of_stay AS (
        SELECT codes.country_name, data.average_stay FROM (
            (SELECT CAST(CAST(i94res AS DOUBLE PRECISION) AS INT) as code, AVG(length_of_stay) as average_stay
            FROM immigration.us_entries WHERE arrival_month={} and arrival_year={} and length_of_stay>=0
            GROUP BY i94res) AS data
            LEFT JOIN immigration.country_codes AS codes
            ON codes.code = data.code));
    COMMIT;
    """
    
    state_trip_reasons = """
    CREATE TABLE outputs.{}{}_state_trip_reasons AS (
        SELECT sc.state_name, tr.trip_reason, data.count FROM (
            (SELECT i94addr as state_code, CAST(CAST(i94visa AS DOUBLE PRECISION) AS INT) as trip_reason_code, count(*) as count
            FROM immigration.us_entries WHERE arrival_month={} and arrival_year={} and i94visa!='nan'
            GROUP BY i94addr, i94visa) AS data
            LEFT JOIN immigration.state_codes AS sc
            ON sc.code = data.state_code
            LEFT JOIN immigration.trip_reason_codes AS tr
            on tr.code = data.trip_reason_code));
    COMMIT;
    """
    
    freqs_and_mean_temps = """
    CREATE TABLE outputs.{}{}_freqs_and_mean_temps AS (
        SELECT codes.country_name, data.visitor_count, temps.mean_temp, temps.stddev_temp FROM (
            immigration.country_codes AS codes
            JOIN (
                SELECT CAST(CAST(i94res AS DOUBLE PRECISION) AS INT) as code, COUNT(*) as visitor_count
                FROM immigration.us_entries WHERE arrival_month={} and arrival_year={}
                GROUP BY i94res) AS data
            ON codes.code = data.code
            JOIN temperature.temp_summary AS temps
            ON codes.country_name = temps.country_name));
    COMMIT;
    """
    
    