""""
Main python script for the automated sensor monitor.
"""

from db import data_5tm
from db import data_mps2
from detection import sensor_state_detector
from outputs import email_reporter
from masterlists import masterlist_json_creator


def main():
    bays = ["E", "C", "W"]


    for bay in bays:
        masterlist_json_creator.main(bay)
        data_5tm.main_data(bay)
        data_mps2.main_data(bay)
        sensor_state_detector.main(bay)
    
    email_reporter.main()


main()