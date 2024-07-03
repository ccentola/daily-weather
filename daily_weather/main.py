import os
import json
import duckdb
import requests
import argparse
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


BASE_URL = "http://api.openweathermap.org/"
OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")
JSON_DIR = "data/json"


def get_coordinates_by_zip(zip_code: str):
    """
    Uses the OpenWeather Geocoding API to retrieve latitude and longitude
    given a zip code or postal code.

    Args:
        zip_code (str): zip code for coordinate lookup

    Returns:
        tuple: latitude, longitude
    """
    # default to local zip if one is not provided
    if zip_code is None:
        zip_code = "85374"

    url = BASE_URL + "geo/1.0/zip"
    params = {"zip": zip_code, "appid": OPEN_WEATHER_API_KEY}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return (data["lat"], data["lon"])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def get_current_weather(lat: float, lon: float):
    """
    Gets the current weather for a given lat, lon using the OpenWeather
    Current Weather API.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        data (dict): API response
    """
    url = BASE_URL + "data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "units": "imperial",
        "appid": OPEN_WEATHER_API_KEY,
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def create_db():
    """
    Database setup. Creates tables if they do not exist.
    """
    conn = duckdb.connect("data/weather.db")

    with open("daily_weather/sql/create_tables.sql", "r") as f:
        sql_file = f.read()

    conn.execute(sql_file)

    conn.close()


def write_result_to_json(result):
    """
    Writes API response to a JSON file for loading into the database.

    Args:
        result: API response

    Returns:
        file_path (str): JSON file to load into the database.
    """

    # make json dir if not exists
    if not os.path.exists(JSON_DIR):
        os.makedirs(JSON_DIR)

    # generate the filename
    time_str = datetime.now().strftime("%Y%m%d%H%M")
    filename = f"{result['id']}_{time_str}.json"

    file_path = os.path.join(JSON_DIR, filename)
    with open(file_path, "w", encoding="utf-8") as output_data:
        json.dump(result, output_data)
    return file_path


def load_data_to_current_weather(json_file):
    """
    Load JSON data from file to the current_weather relation.
    """
    try:
        with duckdb.connect("data/weather.db", read_only=False) as con:
            con.sql(
                f"""
                INSERT INTO current_weather
                    SELECT 
                        id AS location_id, 
                        name AS location_name, 
                        sys.country AS location_country,
                        to_timestamp(sys.sunrise) AS sunrise,
                        to_timestamp(sys.sunrise) AS sunset,
                        coord.lon AS location_lon, 
                        coord.lat AS location_lat, 
                        weather[1].main AS weather_main,
                        weather[1].description AS weather_description, 
                        to_timestamp(dt) AS timestamp_local,
                        main.temp AS temperature,
                        main.feels_like AS temperature_feels_like,
                        main.temp_min AS temperature_min,
                        main.temp_max AS temperature_max,
                        main.pressure AS pressure,
                        main.humidity AS humidity,
                        wind.speed AS wind_speed,
                        wind.deg AS wind_degrees,
                        clouds.all AS clouds
                    FROM '{json_file}'
                """
            )
            con.table("current_weather").show()
    except duckdb.Error as e:
        print("Error loading data to database:", e)


def load_data_to_location():
    """
    Add records to the location relation
    """
    try:
        with duckdb.connect("data/weather.db", read_only=False) as con:
            con.sql(
                """
                INSERT INTO location
                    SELECT DISTINCT
                        location_id, 
                        location_name, 
                        location_country,
                        location_lon, 
                        location_lat
                    FROM current_weather cw
                    ANTI JOIN location l ON l.id = cw.location_id
                """
            )
            con.table("location").show()
    except duckdb.Error as e:
        print("Error loading data to database:", e)


def query_saved_locations():
    try:
        with duckdb.connect("data/weather.db") as con:
            con.execute(
                """
                SELECT DISTINCT name, lat, lon 
                FROM location
                """
            )
            for loc in con.fetchall():
                print(f"Adding record for {loc[0]}...")
                data = get_current_weather(loc[1], loc[2])
                json_file = write_result_to_json(data)
                load_data_to_current_weather(json_file)
                print(f"{loc[0]} record added to current_weather")
    except duckdb.Error as e:
        print(f"DuckDB error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch data from OpenWeatherMap API.")
    parser.add_argument("--zip", type=str, help="zip code to query")

    args = parser.parse_args()

    zip_code = args.zip if args.zip else None
    lat, lon = get_coordinates_by_zip(zip_code)

    if lat:
        data = get_current_weather(lat, lon)
        json_file = write_result_to_json(data)
        create_db()
        load_data_to_current_weather(json_file)
        load_data_to_location()
    else:
        print("Failed to fetch location data.")


if __name__ == "__main__":
    query_saved_locations()
