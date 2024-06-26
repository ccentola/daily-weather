import os
import json
import duckdb
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


BASE_URL = "http://api.openweathermap.org/"
OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")
JSON_DIR = "data/json"


def get_coordinates_by_zip(zip_code: str):
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


def get_current_weather(lat, lon):
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
    # check to see if duckdb table exists; create if it does not
    conn = duckdb.connect("data/weather.db")

    with open("daily_weather/sql/create_tables.sql", "r") as f:
        sql_file = f.read()

    conn.execute(sql_file)

    conn.close()


def write_result_to_json(result):
    """
    Write response results to a JSON file
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


def load_data_to_db(json_file):
    """
    Load JSON data to DuckDB
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


if __name__ == "__main__":
    lat, lon = get_coordinates_by_zip("85374")
    data = get_current_weather(lat, lon)
    json_file = write_result_to_json(data)
    create_db()
    load_data_to_db(json_file)
