import pandas as pd
import json
import config

#converts the excel masterspread sheet into a json file that encodes zone and sensor health information

def load_sheet(f_excel, sheet, csv_list):
    '''
    Loads an excel sheet from a workbook into a pandas dataframe

    Inputs:
    f_excel: a string containing the filepath extension to the excel workbook

    sheet: a string denoting the sheet name to be loaded into the dataframe

    Output:
    A pandas dataframe of the excel sheet
    '''
    df = pd.read_excel(f_excel, sheet_name=sheet, usecols = [0,1,2,3])
    df_csv = df.to_csv(header=False, index=False)
    df_csv = df_csv.split("\n")

    #loads items from csv of execl sheet into csv_list
    #adds height and group to each item
    for item in df_csv:
        if len(item) > 3:
            item_as_list = item.strip("\r").split(",")
            item_as_list.append(sheet)
            try:
                height = int(item_as_list[0].split("_")[2])
            except:
                print(item_as_list)
            
            item_as_list.append(height)
            csv_list.append(item_as_list)

def main_create_csv(f_name_excel, f_out):
    list_items = []
    sheets = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P"]

    #loads each sheet, adds to list_itmes
    for sheet in sheets:
        load_sheet(f_name_excel, sheet, list_items)
    
    #sorts list by group
    list_items.sort(key= lambda item:item[4])

    group_dict = {}

    for item in list_items:
        if item[4] not in group_dict:
            group_dict[item[4]] = {}
        group_dict[item[4]][item[0]] = item
    
    if "west" in f_out:
        del group_dict["O"]["26_-3_2"]
        del group_dict["O"]["26_-2_2"]
    
    
    f_out = open(f_out, "w")
    f_out.write(json.dumps(group_dict))
    

def main(bay):
    f_excel = ""
    f_out = ""

    if bay == "W":
        f_excel = config.path_west_masterlist
        f_out = "masterlists/sensor_status_west.json"
    
    if bay == "C":
        f_excel = config.path_center_masterlist
        f_out = "masterlists/sensor_status_center.json"

    if bay == "E":
        f_excel = config.path_east_masterlist
        f_out = "masterlists/sensor_status_east.json"

    main_create_csv(f_excel, f_out)