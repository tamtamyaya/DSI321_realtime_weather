import pandas as pd
import aiohttp
import asyncio
from datetime import datetime
import pytz
from prefect import flow, task

API_KEY = "f937ef58aa2555b6d76a1119fd917eed"
WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
POLLUTION_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

BATCH_SIZE = 25
WAIT_BETWEEN_BATCHES = 70  # seconds

@task
async def fetch_weather_and_pollution(session, row):
    lat = row["lat"]
    lon = row["lon"]
    district = row["district_en"]
    province = row["province_en"]
    district_id = row["district_id"]

    try:
        params = {"lat": lat, "lon": lon, "appid": API_KEY, "units": "metric"}

        # Weather API call
        async with session.get(WEATHER_URL, params=params) as weather_resp:
            weather_data = await weather_resp.json()

        if weather_data.get("cod") != 200:
            print(f"[ERROR] Missing weather data for {district}: {weather_data}")
            return None

        await asyncio.sleep(2)

        # Pollution API call
        async with session.get(POLLUTION_URL, params=params) as pollution_resp:
            pollution_data = await pollution_resp.json()

        if "list" not in pollution_data or not pollution_data["list"]:
            print(f"[ERROR] Missing pollution data for {district}: {pollution_data}")
            return None

        await asyncio.sleep(2)

        timestamp = datetime.utcnow()
        #timestamp = datetime.now()
        thai_tz = pytz.timezone('Asia/Bangkok')
        localtime = timestamp.astimezone(thai_tz)
        #created_at = dt.replace(tzinfo=thai_tz)

        return {
            "timestamp": timestamp,
            "year": timestamp.year,
            "month": timestamp.month,
            "day": timestamp.day,
            "hour": timestamp.hour,
            "minute": timestamp.minute,
            #"created_at": created_at,
            "district_id": district_id,
            "district": district,
            "province": province,
            "localtime": localtime,
            "weather_main": weather_data["weather"][0]["main"],
            "weather_description": weather_data["weather"][0]["description"],
            "main.temp": weather_data["main"]["temp"],
            "main.temp_min": weather_data["main"]["temp_min"],
            "main.temp_max": weather_data["main"]["temp_max"],
            "main.feels_like": weather_data["main"]["feels_like"],
            "main.pressure": weather_data["main"]["pressure"],
            "main.humidity": weather_data["main"]["humidity"],
            "visibility": weather_data.get("visibility"),
            "wind.speed": weather_data["wind"]["speed"],
            "wind.deg": weather_data["wind"]["deg"],
            "components_co": pollution_data["list"][0]["components"]["co"],
            "components_no": pollution_data["list"][0]["components"]["no"],
            "components_no2": pollution_data["list"][0]["components"]["no2"],
            "components_o3": pollution_data["list"][0]["components"]["o3"],
            "components_so2": pollution_data["list"][0]["components"]["so2"],
            "components_pm2_5": pollution_data["list"][0]["components"]["pm2_5"],
            "components_pm10": pollution_data["list"][0]["components"]["pm10"],
            "components_nh3": pollution_data["list"][0]["components"]["nh3"],
        }

    except Exception as e:
        print(f"[ERROR] Exception for {district}: {e}")
        return None
@flow(name="weather-flow", flow_run_name="weather-run", log_prints=True)
async def main_flow():
    df = pd.read_csv("/home/jovyan/work/districts.csv")
    results = []

    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            tasks = [fetch_weather_and_pollution(session, row) for _, row in batch.iterrows()]
            batch_results = await asyncio.gather(*tasks)
            results.extend([r for r in batch_results if r is not None])
            if i + BATCH_SIZE < len(df):
                print(f"Waiting {WAIT_BETWEEN_BATCHES} seconds before next batch...")
                await asyncio.sleep(WAIT_BETWEEN_BATCHES)

    df_results = pd.DataFrame(results)
    print(df_results)

    # lakeFS credentials from your docker-compose.yml
    ACCESS_KEY = "access_key"
    SECRET_KEY = "secret_key"
    lakefs_endpoint = "http://lakefs-dev:8000/"
    repo = "weather"
    branch = "main"
    path = "weather.parquet"

    lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"
    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {
            "endpoint_url": lakefs_endpoint
        }
    }

    df_results.to_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        partition_cols=['year', 'month', 'day', 'hour'],
    )