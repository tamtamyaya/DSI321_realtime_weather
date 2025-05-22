import streamlit as st
import pyarrow as pa


import pandas as pd
from datetime import datetime, timedelta
import pyarrow.dataset as ds
import s3fs

from dotenv import load_dotenv
import os

import plotly.express as px
import geopandas as gpd


ACCESS_KEY = "access_key"
SECRET_KEY = "secret_key"
lakefs_endpoint = "http://lakefs-dev:8000/"

fs = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY,
    client_kwargs={'endpoint_url': lakefs_endpoint}
)

BASE_DIR = os.getcwd()

### ________________________

# โหลด Data
@st.cache_data(ttl=300)
def load_data(lakefs_path):
    schema = pa.schema([
        ("timestamp", pa.timestamp("ns")),
        ("localtime", pa.timestamp("ns")),
        ("minute", pa.int64()),
        ("district_id", pa.string()),  # เผื่อไว้
        ("components_pm2_5", pa.float64())
    ])

    dataset = ds.dataset(
        lakefs_path,
        format="parquet",
        partitioning="hive",
        filesystem=fs,
        schema=schema
    )
    table = dataset.to_table()
    df = table.to_pandas()

    # ✅ กรองเฉพาะข้อมูลที่มี district_id และอยู่หลังวันที่ 2025-05-18
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df[df["timestamp"] >= pd.Timestamp("2025-05-18")]

    # กรองข้อมูลที่มี district_id ไม่เป็น null
    df = df[df["district_id"].notnull()]
    df["district_id"] = df["district_id"].astype(str)

    df["pm25"] = pd.to_numeric(df["components_pm2_5"], errors="coerce")

    return df



    
    # return df

@st.cache_data()
def load_gdf(geojson_path):
    gdf = gpd.read_file(geojson_path)
    return gdf


####_______________________________________

# อ่านไฟล์ pollution
pollution_path = 'weather/main/weather.parquet'
pollution_df = load_data(pollution_path)
pollution_df = pollution_df.rename(columns={"components_pm2_5": "pm25"})


coord_path = "districts.csv"
df_code = pd.read_csv(coord_path)

# 🔧 ทำให้ทั้ง pollution_df และ df_code ใช้ district_id เป็น string
pollution_df["district_id"] = pollution_df["district_id"].astype(str)
df_code["district_id"] = df_code["district_id"].astype(str)

# 🔁 merge หลังจากแปลง datatype แล้ว
pollution_df = pd.merge(
    pollution_df,
    df_code[["province_th", "district_th", "province_id", "district_id"]],
    on="district_id",
    how="left"
)


# โหลดไฟล์ geojson

province_geojson_path = os.path.join(BASE_DIR, "gadm41_THA_1_clean.geojson")
province_gdf = load_gdf(province_geojson_path)

district_geojson_path = os.path.join(BASE_DIR, "gadm41_THA_2_clean.geojson")
district_gdf = load_gdf(district_geojson_path)


#### _____________________________________________
#__________ Title ________________
# st.set_page_config(page_title="Choropleth Map", page_icon="🗺️")
st.title("แผนที่ค่าฝุ่น PM2.5 รายอำเภอ")

#__________ AQI __________________



# pollution_df["aqi_level"] = pollution_df["pm25"].apply(get_aqi_level)

color_map = {
    "Good": "green",
    "Moderate": "yellow",
    "Unhealthy for Sensitive Groups": "orange",
    "Unhealthy": "red",
    "Very Unhealthy": "purple",
    "Hazardous": "maroon",
    "Very Hazardous": "brown"
}

aqi_continuous_colors = [
    [0.0, "#00e400"],     # Good: Green
    [0.1, "#ffff00"],     # Moderate: Yellow
    [0.2, "#ff7e00"],     # USG: Orange
    [0.5, "#ff0000"],     # Unhealthy: Red
    [1.0, "#8f3f97"]      # Very Unhealthy: Purple
]




#__________ Time Filter __________

# ปรับ timestamp ให้อยู่ในช่วง 15 นาที
pollution_df["localtime"] = pd.to_datetime(pollution_df["localtime"])
pollution_df["local_timestamp_15min"] = pollution_df["localtime"].dt.floor("15min")

# ชุดเวลาที่มีจริง (unique, sorted)
all_times = pollution_df["local_timestamp_15min"].sort_values().unique()

if "selected_time" not in st.session_state:
    st.session_state["selected_time"] = all_times[-1]

selected_time = st.select_slider(
    "เลือกเวลา",
    options=all_times,
    value=st.session_state["selected_time"],
    format_func=lambda x: x.strftime("%Y-%m-%d %H:%M")
)

# ช่วงเวลา +-1 hr
window = pd.Timedelta(hours=1)
start = selected_time - window
end   = selected_time + window

mask = pollution_df["local_timestamp_15min"].between(start, end)
df_window = pollution_df[mask]


#________ Radio ________

# ตั้งค่า default ครั้งแรก
if "selected_level" not in st.session_state:
    st.session_state["selected_level"] = "จังหวัด (Province)"

# แสดง radio พร้อมผูกกับ session_state
level = st.radio(
    "ระดับแผนที่",
    ["จังหวัด (Province)", "อำเภอ (District)"],
    index=["จังหวัด (Province)", "อำเภอ (District)"].index(st.session_state["selected_level"]),
    key="selected_level"  # ผูกกับ session_state
)

#________ MAP ______________

#________ MAP ______________

#________ MAP ______________

#________ MAP ______________

#________ MAP ______________

#________ MAP ______________

#________ MAP ______________

# ✅ วิธีแก้ทีเดียวจบ (เฉพาะส่วน if level == ...)

if level == "จังหวัด (Province)":
    grouped = df_window.groupby(["local_timestamp_15min", "province_id", "province_th"])
    map_df = grouped["pm25"].mean().reset_index()

    # ✅ ลบคอลัมน์ซ้ำและ NaN อย่างปลอดภัย
    map_df = map_df.loc[:, ~map_df.columns.duplicated()].copy()
    map_df.reset_index(drop=True, inplace=True)
    map_df = map_df[map_df["pm25"].notnull()]

    # ✅ แปลงค่า pm25 เป็นระดับ AQI
    bins = [0, 12, 35.4, 55.4, 150.4, 250.4, 350.4, 500.4]
    labels = [
        "Good", "Moderate", "Unhealthy for Sensitive Groups",
        "Unhealthy", "Very Unhealthy", "Hazardous", "Very Hazardous"
    ]
    map_df["aqi_level"] = pd.cut(
        map_df["pm25"],
        bins=bins,
        labels=labels,
        include_lowest=True
    )

    geojson = province_gdf
    locations = "province_id"
    featureidkey = "properties.CC_1"

    hover_name = "province_th"
    hover_data = {
        "province_th": False,
        "pm25": ':.2f',
        "province_id": False,
        "local_timestamp_15min": False
    }
    hovertemplate = (
        "<b>จังหวัด : </b> %{customdata[0]}<br>"
        "<b>PM2.5 : </b> %{customdata[1]:.2f}<extra></extra>"
    )
    customdata = map_df[["province_th", "pm25"]].to_numpy()

elif level == "อำเภอ (District)":
    map_df = (
        df_window
        .sort_values("timestamp")
        .drop_duplicates(["local_timestamp_15min", "district_id"])
    )

    # ✅ ลบคอลัมน์ซ้ำและ NaN อย่างปลอดภัย
    map_df = map_df.loc[:, ~map_df.columns.duplicated()].copy()
    map_df.reset_index(drop=True, inplace=True)
    map_df = map_df[map_df["pm25"].notnull()]

    # ✅ แปลงค่า pm25 เป็นระดับ AQI
    bins = [0, 12, 35.4, 55.4, 150.4, 250.4, 350.4, 500.4]
    labels = [
        "Good", "Moderate", "Unhealthy for Sensitive Groups",
        "Unhealthy", "Very Unhealthy", "Hazardous", "Very Hazardous"
    ]
    map_df["aqi_level"] = pd.cut(
        map_df["pm25"],
        bins=bins,
        labels=labels,
        include_lowest=True
    )

    geojson = district_gdf
    locations = "district_id"
    featureidkey = "properties.CC_2"

    hover_name = "district_th"
    hover_data = {
        "province_th": True,
        "pm25": ':.2f',
        "district_id": False,
        "local_timestamp_15min": False
    }
    hovertemplate = (
        "<b>จังหวัด :</b> %{customdata[0]}<br>"
        "<b>อำเภอ :</b> %{customdata[1]}<br>"
        "<b>PM2.5 :</b> %{customdata[2]:.2f}<extra></extra>"
    )
    customdata = map_df[["province_th", "district_th", "pm25"]].to_numpy()



# plot
fig = px.choropleth_mapbox(
    map_df,
    geojson=geojson,
    locations=locations,
    featureidkey=featureidkey,
    color="pm25",
    color_continuous_scale="YlOrRd",
    # color_continuous_scale=aqi_continuous_colors,
    range_color=(0, 100),
    mapbox_style="carto-positron",
    zoom=5,
    center={"lat": 13.5, "lon": 100.5},
    opacity=0.6,
    labels={"pm25": "ค่าฝุ่น PM2.5 ", "province_th": "จังหวัด "},
    # hover_name=hover_name,
    # hover_data=hover_data,
    animation_frame="local_timestamp_15min"

)

# แก้ pop up
fig.update_traces(
    hovertemplate=hovertemplate,
    customdata=customdata,
)

st.plotly_chart(fig, use_container_width=True)

#######
# Score Card
# Time Serie
# Interactive
# Alert
# date
# backgroud
# UI
###