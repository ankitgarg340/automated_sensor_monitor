"""
This program handles email communication that reports sensor health states. It uses the outputs of the sensor_state_detector
and generates an html which is sent via email. It is currently configured only for the 5TM and MPS2 sensor types.
"""

import pandas as pd
import smtplib
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path
import datetime
import json

import config

# =====================
# Config elements — edit these as needed
# =====================
ZONE_CSV_C_5TM = "outputs/zone_health_C.csv"
SENSOR_CSV_C_5TM = "outputs/sensor_health_C.csv"

ZONE_CSV_E_5TM = "outputs/zone_health_E.csv"
SENSOR_CSV_E_5TM = "outputs/sensor_health_E.csv"

ZONE_CSV_W_5TM = "outputs/zone_health_W.csv"
SENSOR_CSV_W_5TM = "outputs/sensor_health_W.csv"

ZONE_CSV_C_MPS = "outputs/zone_health_C_mps.csv"
SENSOR_CSV_C_MPS = "outputs/sensor_health_C_mps.csv"

ZONE_CSV_E_MPS = "outputs/zone_health_E_mps.csv"
SENSOR_CSV_E_MPS = "outputs/sensor_health_E_mps.csv"

ZONE_CSV_W_MPS = "outputs/zone_health_W_mps.csv"
SENSOR_CSV_W_MPS = "outputs/sensor_health_W_mps.csv"

JSON_W = "masterlists/sensor_status_west.json"
JSON_C = "masterlists/sensor_status_center.json"
JSON_E = "masterlists/sensor_status_east.json"

FROM_ADDR = config.email_from_adr
TO_ADDRS = config.email_to_adrs
SUBJECT = "Zone & Sensor Health Report"

SMTP_HOST = config.email_smtp_host
SMTP_PORT = 587
SMTP_USER = config.email_smtp_un
SMTP_PASS = config.email_smtp_pw
USE_SSL = False       # True for implicit SSL (465)
USE_STARTTLS = True   # True for STARTTLS (587)

ATTACH_DIR = "outputs"  # where to save the .html file

def find_down_sensors(sensor_df, masterlist_json):
    #finds which sensors are down, and returns a pandas df of only those sensors
    #function also handles connecting all sensors to their approriate zones

    with open(masterlist_json, "r") as f:
        zone_map = json.load(f)

    for index,row in sensor_df.iterrows():

        sensor_zone = ""
        for zone in zone_map:
            sensor_loc = row["sensor name"].split("_")
            sensor_loc_string = sensor_loc[1] + "_" + sensor_loc[2] + "_" + sensor_loc[3]
            if sensor_loc_string in zone_map[zone]:
                sensor_zone = zone
        
        sensor_df.at[index, "zone"] = sensor_zone



    filtered_df = sensor_df[sensor_df["state"] == 1].copy()
    filtered_df["state"] = filtered_df["state"].apply(lambda x: "sensor down" if x == 1 else x)
    filtered_df["zone"] = sensor_df["zone"]
    return filtered_df

def find_sometimes_sensors(sensor_df):
    #finds sensors which come in and out of reporting values, returns them in a pandas df

    filtered_df = sensor_df[sensor_df["state"] == 2].copy()
    filtered_df["state"] = filtered_df["state"].apply(lambda x: "in and out of data" if x == 2 else x)
    return filtered_df

def find_outlier_sensors(sensor_df):
    #finds sensors which are reporting outliers, and returns them in a pandas df

    filtered_df = sensor_df[sensor_df["state"] == 3].copy()
    filtered_df["state"] = filtered_df["state"].apply(lambda x: "outliers present" if x == 3 else x)
    return filtered_df

def readable_zone_df(zone_df):
    #converts a dataframe so that instead of using the integer encoding of health state, it reports it as a string

    readable_df = zone_df.copy()
    readable_df["state"] = readable_df["state"].apply(lambda x: "alive" if x == 1 else x)
    readable_df["state"] = readable_df["state"].apply(lambda x: "down" if x == 0 else x)

    return readable_df

def build_html(zone_df_c_5tm, sensor_df_c_5tm, zone_df_e_5tm, sensor_df_e_5tm, zone_df_w_5tm, sensor_df_w_5tm, zone_df_c_mps, sensor_df_c_mps, zone_df_e_mps, sensor_df_e_mps, zone_df_w_mps, sensor_df_w_mps):
    #builds HTML that is sent in the email
    #each of the parameters are pandas dfs for 5tms, mps2s, 5tm zones, and mps2 zones for each hillslope
    
    today = datetime.date.today()
    start = today - datetime.timedelta(1)
    end = today 


    #startTime = '2025/07/28 00:00'
    startTime = start.strftime("%Y/%m/%d 00:00")
    
    #endTime = '2025/07/29 00:00'
    endTime = end.strftime("%Y/%m/%d 00:00")


    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
      <h2>Zone & Sensor Health Report</h2>
      <p>Generated using database outputs from {startTime} to {endTime}</p>
      <hr style="height:5px; background:#000; color: #000; border-width:0">

      <h1>East Bay</h1>
      <h2>5TM report - East Bay</h2>
      <h3>Zones status - 5TM</h3>
      {readable_zone_df(zone_df_e_5tm).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are down.</h3>
      {find_down_sensors(sensor_df_e_5tm, JSON_E).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are coming in and out of reporting values.</h3>
      {find_sometimes_sensors(sensor_df_e_5tm).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are reporting outlier values</h3>
      {find_outlier_sensors(sensor_df_e_5tm).to_html(header=True, index=False, border=2)}
      
      <hr style="height:5px; background:#000; color: #000; border-width:0">
      <h2>MPS2 report - East Bay</h2>
      <h3>Zones status - MPS2</h3>
      {readable_zone_df(zone_df_e_mps).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are down.</h3>
      {find_down_sensors(sensor_df_e_mps, JSON_E).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are coming in and out of reporting values.</h3>
      {find_sometimes_sensors(sensor_df_e_mps).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are reporting outlier values.</h3>
      {find_outlier_sensors(sensor_df_e_mps).to_html(header=True, index=False, border=2)}
      <hr style="height:5px; background:#000; color: #000; border-width:0">
      
      <h1>Center Bay</h1>
      <h2>5TM report - Center Bay</h2>
      <h3>Zones status - 5TM</h3>
      {readable_zone_df(zone_df_c_5tm).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are down.</h3>
      {find_down_sensors(sensor_df_c_5tm, JSON_C).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are coming in and out of reporting values.</h3>
      {find_sometimes_sensors(sensor_df_c_5tm).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are reporting outlier values.</h3>
      {find_outlier_sensors(sensor_df_c_5tm).to_html(header=True, index=False, border=2)}
    
      <hr style="height:5px; background:#000; color: #000; border-width:0">
      <h2>MPS2 report - Center Bay</h2>      
      <h3>Zones status - MPS2</h3>
      {readable_zone_df(zone_df_c_mps).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are down.</h3>
      {find_down_sensors(sensor_df_c_mps, JSON_C).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are coming in and out of reporting values.</h3>
      {find_sometimes_sensors(sensor_df_c_mps).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are reporting outlier values.</h3>
      {find_outlier_sensors(sensor_df_c_mps).to_html(header=True, index=False, border=2)}
      <hr style="height:5px; background:#000; color: #000; border-width:0">

      <h1>West Bay</h1>
      <h2>5TM report - West Bay</h2> 
      <h3>Zones status - 5TM</h3>
      {readable_zone_df(zone_df_w_5tm).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are down.</h3>
      {find_down_sensors(sensor_df_w_5tm, JSON_W).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are coming in and out of reporting values.</h3>
      {find_sometimes_sensors(sensor_df_w_5tm).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are reporting outlier values</h3>
      {find_outlier_sensors(sensor_df_w_5tm).to_html(header=True, index=False, border=2)}

      <hr style="height:5px; background:#000; color: #000; border-width:0">
      <h2>MPS2 report - West Bay</h2>
      <h3>Zones status - MPS2</h3>
      {readable_zone_df(zone_df_w_mps).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are down.</h3>
      {find_down_sensors(sensor_df_w_mps, JSON_W).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are coming in and out of reporting values.</h3>
      {find_sometimes_sensors(sensor_df_w_mps).to_html(header=True, index=False, border=2)}
      <h3>Sensors that are reporting outlier values</h3>
      {find_outlier_sensors(sensor_df_w_mps).to_html(header=True, index=False, border=2)}
    </body>
    </html>
    """
    return html

def send_email(html):
    # --- prepare an .html file on disk to attach ---
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    filename = f"zone_sensor_health_report.html"
    outpath = Path(ATTACH_DIR) / filename
    outpath.parent.mkdir(parents=True, exist_ok=True)
    outpath.write_text(html, encoding="utf-8")  # file artifact you can keep

    # --- build the email with both HTML body & attachment ---
    msg = EmailMessage()
    msg["Subject"] = SUBJECT
    msg["From"] = FROM_ADDR
    msg["To"] = ", ".join(TO_ADDRS)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()

    # Plain-text fallback + HTML body
    msg.set_content("This email contains HTML content. See the attached HTML file for a copy.")
    msg.add_alternative(html, subtype="html")

    # Attach the saved HTML file
    msg.add_attachment(
        outpath.read_bytes(),
        maintype="text",
        subtype="html",
        filename=filename
    )

    # --- send ---
    if USE_SSL:
        import ssl
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            if USE_STARTTLS:
                server.starttls()
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)


def main():
    zone_df_c_5tm = pd.read_csv(ZONE_CSV_C_5TM)
    sensor_df_c_5tm = pd.read_csv(SENSOR_CSV_C_5TM)

    zone_df_e_5tm = pd.read_csv(ZONE_CSV_E_5TM)
    sensor_df_e_5tm = pd.read_csv(SENSOR_CSV_E_5TM)

    zone_df_w_5tm = pd.read_csv(ZONE_CSV_W_5TM)
    sensor_df_w_5tm = pd.read_csv(SENSOR_CSV_W_5TM)

    zone_df_c_mps = pd.read_csv(ZONE_CSV_C_MPS)
    sensor_df_c_mps = pd.read_csv(SENSOR_CSV_C_MPS)

    zone_df_e_mps = pd.read_csv(ZONE_CSV_E_MPS)
    sensor_df_e_mps = pd.read_csv(SENSOR_CSV_E_MPS)

    zone_df_w_mps = pd.read_csv(ZONE_CSV_W_MPS)
    sensor_df_w_mps = pd.read_csv(SENSOR_CSV_W_MPS)

    html = build_html(zone_df_c_5tm, sensor_df_c_5tm, zone_df_e_5tm, sensor_df_e_5tm, zone_df_w_5tm, sensor_df_w_5tm, zone_df_c_mps, sensor_df_c_mps, zone_df_e_mps, sensor_df_e_mps, zone_df_w_mps, sensor_df_w_mps)

    send_email(html)
    print("Email sent.")