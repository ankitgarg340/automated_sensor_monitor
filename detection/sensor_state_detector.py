'''
This program categorizes 5TMs and MPS2 sensors into given states. It uses the database outputs of the sensors
and uses these values to put into one of 5 state (see header of determine health functions). It will find the state
of each sensor zone. It will report these values into csvs in the outputs folder.
'''

import csv
import json
import copy

def generate_sensor_dict(filename_csv):
    '''
    Takes a CSV of queried sensor outputs from the research database and loads it into a dictionary

    Inputs:
    filename_csv: The filename/path to the csv file that holds the queried sensor information

    Outputs:
    sensor_dict: A dictionary that contains the sensor name as a key and an array of floats as the values
    '''
    sensor_dict = {}
    with open(filename_csv, "r") as sensorfile:
        reader = csv.reader(sensorfile)
        for line in reader:
            sensor_dict[line[0]] = []

            for val in line[1:]:
                try:
                    sensor_dict[line[0]].append(float(val))
                except:
                    sensor_dict[line[0]] = []
    
    return sensor_dict

def generate_json_dict(filename_json):
    '''
    loads the json file encoding the sdi12 masterlist into a python dictionary

    Inputs:
    filename_json: The filename/path to the json file that holds the sdi12 masterlist encoded

    Outputs:
    sensors_dict: a dictionary of the json file data
    '''
    sensors_dict = {}

    with open(filename_json, "r") as jsonfile:
        sensors_dict = json.load(jsonfile)
    
    return sensors_dict
    

def determine_health_5tm(outputs):
    #0 = sensor removed
    #1 = sensor fully dead
    #2 = in and out of data
    #3 = outlier data point present
    #4 = sensor healthy
    
    '''
    determines what state (see defined integer states above) a 5tm sensor is in based off its database output

    Inputs:
    outputs: an array of floats that contains the sensor's data from the database

    Outputs:
    an integer encoding the sensors health state
    '''

    all_null = True
    has_null = False
    has_outlier = False

    for point in outputs:

        if point != -9999:
            all_null = False

        elif point == -9999:
            has_null = True
        
        if (point < -2 or point >= 102) and (point != -9999):
            has_outlier = True
    
    if all_null:
        return 1
    
    if has_null:
        return 2
    
    if has_outlier:
        return 3
    
    return 4

def determine_health_mps(outputs):
    #0 = sensor removed
    #1 = sensor fully dead
    #2 = in and out of data
    #3 = outlier data point present
    #4 = sensor healthy

    '''
    determines what state (see defined integer states above) an MPS2 sensor is in based off its database output

    Inputs:
    outputs: an array of floats that contains the sensor's data from the database

    Outputs:
    an integer encoding the sensors health state
    '''

    all_null = True
    has_null = False
    has_outlier = False

    for point in outputs:

        if point != -9999:
            all_null = False

        elif point == 9999:
            has_null = True
        
        if (point < -750 or point >= -3.75) and (point != -9999):
            has_outlier = True
    
    if all_null:
        return 1
    
    if has_null:
        return 2
    
    if has_outlier:
        return 3
    
    return 4


def build_zone_dict(outputs_dict, masterlist_dict, bay, sensor_type):
    '''
    masterlist_dict_copy = {{zone a: {{sensor 1: 0}, {sensor 2: 3}, ...}, {zone b: {{sensor 1: 0}, {sensor 2: 3}, ...}, ...}

    Creates a dictionary of the sensor state values that also encodes zones. See above line for details

    Inputs:
    outputs_dict: dictionary of sensors and they're associated health state

    masterlist_dict: dictionary that encodes masterlist info

    sensor_type: string of either "5TM" or "MPS-2"

    Outputs:
    dictionary of dictionaries that encodes sensor states by zone
    '''
    masterlist_dict_copy = copy.deepcopy(masterlist_dict)

    for zone in masterlist_dict_copy:
        for sensor in masterlist_dict_copy[zone]:
            masterlist_dict_copy[zone][sensor] = outputs_dict[get_sensor_name(sensor, bay, sensor_type)]

    return masterlist_dict_copy

def generate_zone_health_dict(health_dict):
    #0 = zone down
    #1 = zone alive

    """
    creates a dictionary that encodes the state of each sensor zone. See above for defined integer state values

    input:
    health_dict: dictionary of dictionaries of sensor health information encoded by zone (see build_zone_dict)

    output:
    dictionary of zone health information, with defined integer states above
    """

    zone_health_dict = {}

    for zone in health_dict:

        all_dead = True
        for sensor in health_dict[zone]:
            if health_dict[zone][sensor] != 1 or health_dict[zone][sensor] != 0:
                all_dead = False

        if all_dead:
            zone_health_dict[zone] = 0
        else:
            zone_health_dict[zone] = 1
    
    return zone_health_dict
    

def compare_sensors_5tm(sensor_dict, json_dict):
    """
    compares the sensor health dict to the masterlist for 5TMs, and will filter out sensors which have already been removed by setting their state to 0

    input:
    sensor_dict: dictionary of dictionaries of sensor health information encoded by zone (see build_zone_dict)
    json_dict: dictionary of masterlist

    output:
    None, modifies sensor_dict
    """
    for sensor in sensor_dict:
        if sensor_dict[sensor] == 1:
            for group in json_dict:
                if get_sensor_loc(sensor) in json_dict[group]:
                    if len(json_dict[group][get_sensor_loc(sensor)][3]) != 0:
                        sensor_dict[sensor] = 0
                        #print(json_dict[group][get_sensor_loc(sensor)])
                        #print(sensor + ":" + str(sensor_dict[sensor]))

def compare_sensors_mps(sensor_dict, json_dict):
    """
    compares the sensor health dict to the masterlist for MPS2, and will filter out sensors which have already been removed by setting their state to 0

    input:
    sensor_dict: dictionary of dictionaries of sensor health information encoded by zone (see build_zone_dict)
    json_dict: dictionary of masterlist

    output:
    None, modifies sensor_dict
    """
    for sensor in sensor_dict:
        if sensor_dict[sensor] == 1:
            for group in json_dict:
                if get_sensor_loc(sensor) in json_dict[group]:
                    if len(json_dict[group][get_sensor_loc(sensor)][2]) != 0:
                        sensor_dict[sensor] = 0
                        #print(json_dict[group][get_sensor_loc(sensor)])
                        #print(sensor + ":" + str(sensor_dict[sensor]))
        
def get_sensor_loc(sensor_name):
    loc_items = sensor_name.split("_")
    return loc_items[1] + "_" + loc_items[2] + "_" + loc_items[3]

def get_sensor_name(sensor_loc, bay, type):
    return "LEO-" + bay + "_" + sensor_loc + "_" + type


def main(bay):
    #for 5TM checking
    f_name_json = "masterlists/sensor_status_center.json"
    f_name_csv = "masterlists/true_data_center.csv"

    if bay == "E":
        f_name_json = "masterlists/sensor_status_east.json"
        f_name_csv = "masterlists/true_data_east.csv"


    if bay == "C":
        f_name_json = "masterlists/sensor_status_center.json"
        f_name_csv = "masterlists/true_data_center.csv"


    if bay == "W":
        f_name_json = "masterlists/sensor_status_west.json"
        f_name_csv = "masterlists/true_data_west.csv"
    
    sensors = generate_sensor_dict(f_name_csv)
    json_dict = generate_json_dict(f_name_json)

    sensor_health = {}
    zone_health = {}

    for k,v in sensors.items():
        sensor_health[k] = determine_health_5tm(v)

    compare_sensors_5tm(sensor_health, json_dict)

    sensor_health_by_zone = build_zone_dict(sensor_health, json_dict, bay, "5TM")

    zone_health = generate_zone_health_dict(sensor_health_by_zone)

    if bay == "W":
        f_out_zone = "outputs/zone_health_W.csv"
        f_out_sensors = "outputs/sensor_health_W.csv"

    if bay == "E":
        f_out_zone = "outputs/zone_health_E.csv"
        f_out_sensors = "outputs/sensor_health_E.csv"
    
    if bay == "C":
        f_out_zone = "outputs/zone_health_C.csv"
        f_out_sensors = "outputs/sensor_health_C.csv"

    with open(f_out_zone, "w") as zonefile:
        zonefile.write("zone,state\n")
        for k,v in zone_health.items():
            zonefile.write(k + "," + str(v) + "\n")
        
    with open(f_out_sensors, "w") as sensorfile:
        sensorfile.write("sensor name,state\n")
        for k,v in sensor_health.items():
            sensorfile.write(k + "," + str(v) + "\n")


    #for MPS2
    f_name_csv_mps = "masterlists/true_data_center_mps.csv"

    if bay == "E":
        f_name_csv_mps = "masterlists/true_data_east_mps.csv"


    if bay == "C":
        f_name_csv_mps = "masterlists/true_data_center_mps.csv"


    if bay == "W":
        f_name_csv_mps = "masterlists/true_data_west_mps.csv"

    sensors_mps = generate_sensor_dict(f_name_csv_mps)
    json_dict_mps = generate_json_dict(f_name_json)

    sensor_health_mps = {}
    zone_health_mps = {}

    for k,v in sensors_mps.items():
        sensor_health_mps[k] = determine_health_mps(v)

    compare_sensors_mps(sensor_health_mps, json_dict_mps)

    sensor_health_by_zone_mps = build_zone_dict(sensor_health_mps, json_dict_mps, bay, "MPS-2")

    zone_health_mps = generate_zone_health_dict(sensor_health_by_zone_mps)


    if bay == "W":
        f_out_zone_mps = "outputs/zone_health_W_mps.csv"
        f_out_sensors_mps = "outputs/sensor_health_W_mps.csv"

    if bay == "E":
        f_out_zone_mps = "outputs/zone_health_E_mps.csv"
        f_out_sensors_mps = "outputs/sensor_health_E_mps.csv"
    
    if bay == "C":
        f_out_zone_mps = "outputs/zone_health_C_mps.csv"
        f_out_sensors_mps = "outputs/sensor_health_C_mps.csv"

    with open(f_out_zone_mps, "w") as zonefile:
        zonefile.write("zone,state\n")
        for k,v in zone_health_mps.items():
            zonefile.write(k + "," + str(v) + "\n")
        
    with open(f_out_sensors_mps, "w") as sensorfile:
        sensorfile.write("sensor name,state\n")
        for k,v in sensor_health_mps.items():
            sensorfile.write(k + "," + str(v) + "\n")
