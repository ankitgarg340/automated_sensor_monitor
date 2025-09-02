SENSOR MONITOR PROJECT
Email: ankitgarg@arizona.edu

Automated Sensor Monitor
Author: Ankit Garg (email: ankitgarg@arizona.edu or ankit195300@gmail.com)
Date created: 09/01/2025

Overview
The Automated Sensor Monitor application aims to automate the process of monitoring the sensors of the LEO hillslopes. In brief, the application queries the research database to retrieve sensor outputs for specific sensor types. The application then runs the sensor outputs through the sensor state detection algorithm to determine the state of the sensors that are queried. Finally, the application sends an email that reports the state of the individual sensors (as well as the state of each sensor zone for the SDI12 sensors). This application can be set to run automatically at a given time using a task scheduler.

Currently, the application is set up to work for the 5TM and MPS2 sensor types. The sensor state detection algorithm is set up to detect 4 different states that each sensor could be in, which are sensor dead (state 1), sensor in and out reporting data (state 2), sensor reporting outliers (state 3), and finally sensor healthy (state 4).

The application is written in python and uses the oracle instant client to query the database. Database queries are handled by SQL. The database queries for each sensor type and each hillslope are stored in individual files in the db folder of the application.


Required Libraries:
-Pandas (https://pandas.pydata.org)
-smtplib (https://docs.python.org/3/library/smtplib.html)
-email
-oracledb (https://python-oracledb.readthedocs.io/en/latest/)
-Oracle Instant Client (Version 23_7 included in lib folder of source code)

The easiest way to install pandas is through pip. Open a terminal application (powershell for windows or terminal for mac) and type "pip install pandas" to install

Source Code Folder Breakdown:
db - contains database query code and SQL query strings

detection - contains code for the invalid sensor detection algorithm

lib - contains Oracle Instant Client package used in database query code

masterlists - contains code and source files for the excel master spreadsheet for each bay (needs to be updated with most recently master spreadsheet)

outputs - contains the output log files of the invalid sensor detection algorithm as well as the email reporting python script

Note from Matej:
VWC real range should be between 0 and 50, which given by soil porosity, but 0-100 interval is fine.
Negative VWC might be good, if they are consistent. Many sensors drifted over time and measure value is smaller than it should be. In this case sensor needs to be recalibrated.
The other way to eliminate outliers is to compute AVG St deviation (SD) and all values >/< (AVG +/- 3*SD) are outliers. The window to compute AVG and SD can be +/- 3-4 hours.

