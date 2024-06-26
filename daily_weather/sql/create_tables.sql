CREATE TABLE IF NOT EXISTS current_weather (
    location_id INTEGER NOT NULL, 
    location_name VARCHAR NOT NULL, 
    location_country VARCHAR NOT NULL,
    sunrise TIMESTAMPTZ NOT NULL,
    sunset TIMESTAMPTZ NOT NULL,
    location_lon DOUBLE NOT NULL, 
    location_lat DOUBLE NOT NULL, 
    weather_main VARCHAR NOT NULL,
    weather_description VARCHAR NOT NULL, 
    timestamp_local TIMESTAMPTZ NOT NULL,
    temperature DOUBLE NOT NULL,
    temperature_feels_like DOUBLE NOT NULL,
    temperature_min DOUBLE NOT NULL,
    temperature_max DOUBLE NOT NULL,
    pressure INTEGER NOT NULL,
    humidity INTEGER NOT NULL,
    wind_speed INTEGER NOT NULL,
    wind_degrees INTEGER NOT NULL,
    clouds INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER NOT NULL, 
    name VARCHAR NOT NULL, 
    country VARCHAR NOT NULL,
    lon DOUBLE NOT NULL, 
    lat DOUBLE NOT NULL
)