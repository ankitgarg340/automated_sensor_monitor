'''
This program queries sensor data from the MPS-2 sensors. It dynamically queries data from the previous 24 hour day
period, and queries all MPS-2 sensors in a given hillslope. This program then writes the database output
to a csv file in the outputs folder.
'''

import oracledb

from db import query_strings_E
from db import query_strings_W
from db import query_strings_C
import config

import csv

import datetime


#connecting to downloaded oracle client
def main_data(bay):
    #change lib_dir location 
    oracledb.init_oracle_client(lib_dir=r"lib\instantclient_23_7")
    oracledb.defaults.arraysize = 100

    directory = "data"

    #generates a valid start and end time of query using dynamic dates
    today = datetime.date.today()
    start = today - datetime.timedelta(1)
    end = today 


    #startTime = 'YYYY/MM/DD HH:MM'
    startTime = start.strftime("%Y/%m/%d 00:00")
    
    #endTime = 'YYYY/MM/DD HH:MM'
    endTime = end.strftime("%Y/%m/%d 00:00")

    print(startTime)
    print(endTime)

    slope = ""

    #changes slope being queried depending on bay param
    if bay == "W":
        slope = "leo_west"

    if bay == "C":
        slope = "leo_center"
    
    if bay == "E":
        slope = "leo_east"

    # database connection values stored in config file
    pw = config.db_pw
    un = config.db_un
    host = config.db_host
    sn = config.db_sn
    
    connection = oracledb.connect(user=un, password=pw, host=host, port=1521, sid=sn)
    cursor = connection.cursor()
    print("Successfully connected to Oracle Database")

    #sets the correct sensor query strings for the bay
    sensorCodes = (query_strings_C.query_MPS2_order).split(",")
    sensorPivot = (query_strings_C.query_MPS2) #1 as s1,...
    sensorIDs = (query_strings_C.query_MPS2_ids) #1,2,3,...

    if bay == "W":
        sensorCodes = (query_strings_W.query_MPS2_order).split(",")
        sensorPivot = (query_strings_W.query_MPS2) #1 as s1,...
        sensorIDs = (query_strings_W.query_MPS2_ids) #1,2,3,...

    if bay == "C":
        sensorCodes = (query_strings_C.query_MPS2_order).split(",")
        sensorPivot = (query_strings_C.query_MPS2) #1 as s1,...
        sensorIDs = (query_strings_C.query_MPS2_ids) #1,2,3,...
    
    if bay == "E":
        sensorCodes = (query_strings_E.query_MPS2_order).split(",")
        sensorPivot = (query_strings_E.query_MPS2) #1 as s1,...
        sensorIDs = (query_strings_E.query_MPS2_ids) #1,2,3,...


    # prepare and execute sql query to select Water Potential
    variableID = 7    # Wpa = 7
    sql_vwc = """
        SELECT * FROM 
        (select to_char(localdatetime, 'YYYY/MM/DD HH24:MI') as DateTime, datavalue, sensorid 
        from {}.datavalues where sensorid in ({}) and variableid = {} 
        and localdatetime >= to_date('{}', 'YYYY/MM/DD HH24:MI') 
        and localdatetime < to_date('{}', 'YYYY/MM/DD HH24:MI')) 
        PIVOT (AVG(datavalue) FOR sensorid IN ({})) 
        order by DateTime
        """
    
    sqle = sql_vwc.format(slope, sensorIDs, variableID, startTime, endTime, sensorPivot)

    resultsVWC = cursor.execute(sqle)

    #dictionary to store results
    results_dict = {} 

    #iterates through rows of data(rows are datetime, s1, s2, s3, ....)
    for row in resultsVWC:

        #takes each value in the row, and adds it to appropriate sensor in dictionary
        for i in range(1, len(row)):
            if sensorCodes[i-1] not in results_dict:
                results_dict[sensorCodes[i-1]] = [row[i]]
            else:
                results_dict[sensorCodes[i-1]].append(row[i])

    #sets correct output file
    if bay == "E":
        fname_out = "masterlists/true_data_east_mps.csv"
    if bay == "C":
        fname_out = "masterlists/true_data_center_mps.csv"
    if bay == "W":
        fname_out = "masterlists/true_data_west_mps.csv"

    #writes database ouput to output file
    with open(fname_out, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for key, values in results_dict.items():

            #corrects error in last row
            if "MPS-2" not in key:
                key = key + "2"
            writer.writerow([key] + values)

    cursor.close()
    connection.close()