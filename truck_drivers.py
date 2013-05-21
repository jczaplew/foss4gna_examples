## Script to gather the number of truck driver and warehousing jobs in each state
## and insert it into a MySQL database. Intended to be run daily.

## Requires MySQLdb, which can be downloaded here: https://pypi.python.org/pypi/MySQL-python/1.2.4

## Also requires a careerbuilder.com API key, which can be found here:
## http://api.careerbuilder.com/RequestDevKey.aspx

## John J Czaplewski | jczaplew@gmail.com | Februrary, 2013

import MySQLdb
import urllib2
import os
import xml.etree.ElementTree as ET
import datetime
import calendar
import time

# Find today's date and assemble a variable to hold it for use later
now = datetime.datetime.now()
datestamp = calendar.month_name[now.month] + str(now.day) + str(now.year)

states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
statesFull = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New_Hampshire', 'New_Jersey', 'New_Mexico', 'New_York', 'North_Carolina', 'North_Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode_Island', 'South_Carolina', 'South_Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West_Virginia', 'Wisconsin', 'Wyoming']

# Connect to a MySQL DB
db = MySQLdb.connect(host="#####", user="#####", passwd="#####", db="#####")

# Create cursor to execture queries against the DB
cur = db.cursor() 

# Uncomment the following lines if being run for the first time
##createTable1 = "CREATE TABLE IF NOT EXISTS `truck_data` (`date` varchar(14) DEFAULT NULL,`Alabama` int(3) DEFAULT NULL,`Alaska` int(3) DEFAULT NULL,`Arizona` int(3) DEFAULT NULL,`Arkansas` int(3) DEFAULT NULL,`California` int(4) DEFAULT NULL,`Colorado` int(3) DEFAULT NULL,`Connecticut` int(3) DEFAULT NULL,`Delaware` int(2) DEFAULT NULL,`Florida` int(3) DEFAULT NULL,`Georgia` int(3) DEFAULT NULL,`Hawaii` int(2) DEFAULT NULL,`Idaho` int(3) DEFAULT NULL,`Illinois` int(3) DEFAULT NULL,`Indiana` int(3) DEFAULT NULL,`Iowa` int(3) DEFAULT NULL,`Kansas` int(3) DEFAULT NULL,`Kentucky` int(3) DEFAULT NULL,`Louisiana` int(3) DEFAULT NULL,`Maine` int(3) DEFAULT NULL,`Maryland` int(3) DEFAULT NULL,`Massachusetts` int(3) DEFAULT NULL,`Michigan` int(3) DEFAULT NULL,`Minnesota` int(3) DEFAULT NULL,`Mississippi` int(3) DEFAULT NULL,`Missouri` int(3) DEFAULT NULL,`Montana` int(2) DEFAULT NULL,`Nebraska` int(3) DEFAULT NULL,`Nevada` int(3) DEFAULT NULL,`New_Hampshire` int(3) DEFAULT NULL,`New_Jersey` int(3) DEFAULT NULL,`New_Mexico` int(3) DEFAULT NULL,`New_York` int(3) DEFAULT NULL,`North_Carolina` int(3) DEFAULT NULL,`North_Dakota` int(3) DEFAULT NULL,`Ohio` int(4) DEFAULT NULL,`Oklahoma` int(3) DEFAULT NULL,`Oregon` int(3) DEFAULT NULL,`Pennsylvania` int(3) DEFAULT NULL,`Rhode_Island` int(3) DEFAULT NULL,`South_Carolina` int(3) DEFAULT NULL,`South_Dakota` int(3) DEFAULT NULL,`Tennessee` int(4) DEFAULT NULL,`Texas` int(4) DEFAULT NULL,`Utah` int(3) DEFAULT NULL,`Vermont` int(3) DEFAULT NULL,`Virginia` int(3) DEFAULT NULL,`Washington` int(3) DEFAULT NULL,`West_Virginia` int(3) DEFAULT NULL,`Wisconsin` int(3) DEFAULT NULL,`Wyoming` int(2) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
##cur.execute(createTable1)

##createTable2 = "CREATE TABLE IF NOT EXISTS `warehouse_data` (`date` varchar(14) DEFAULT NULL,`Alabama` int(3) DEFAULT NULL,`Alaska` int(2) DEFAULT NULL,`Arizona` int(3) DEFAULT NULL,`Arkansas` int(3) DEFAULT NULL,`California` int(4) DEFAULT NULL,`Colorado` int(3) DEFAULT NULL,`Connecticut` int(3) DEFAULT NULL,`Delaware` int(2) DEFAULT NULL,`Florida` int(3) DEFAULT NULL,`Georgia` int(3) DEFAULT NULL,`Hawaii` int(2) DEFAULT NULL,`Idaho` int(3) DEFAULT NULL,`Illinois` int(3) DEFAULT NULL,`Indiana` int(3) DEFAULT NULL,`Iowa` int(3) DEFAULT NULL,`Kansas` int(3) DEFAULT NULL,`Kentucky` int(3) DEFAULT NULL,`Louisiana` int(3) DEFAULT NULL,`Maine` int(3) DEFAULT NULL,`Maryland` int(3) DEFAULT NULL,`Massachusetts` int(3) DEFAULT NULL,`Michigan` int(3) DEFAULT NULL,`Minnesota` int(3) DEFAULT NULL,`Mississippi` int(3) DEFAULT NULL,`Missouri` int(3) DEFAULT NULL,`Montana` int(3) DEFAULT NULL,`Nebraska` int(3) DEFAULT NULL,`Nevada` int(3) DEFAULT NULL,`New_Hampshire` int(3) DEFAULT NULL,`New_Jersey` int(3) DEFAULT NULL,`New_Mexico` int(3) DEFAULT NULL,`New_York` int(3) DEFAULT NULL,`North_Carolina` int(3) DEFAULT NULL,`North_Dakota` int(3) DEFAULT NULL,`Ohio` int(3) DEFAULT NULL,`Oklahoma` int(3) DEFAULT NULL,`Oregon` int(3) DEFAULT NULL,`Pennsylvania` int(3) DEFAULT NULL,`Rhode_Island` int(3) DEFAULT NULL,`South_Carolina` int(3) DEFAULT NULL,`South_Dakota` int(4) DEFAULT NULL,`Tennessee` int(4) DEFAULT NULL,`Texas` int(4) DEFAULT NULL,`Utah` int(3) DEFAULT NULL,`Vermont` int(3) DEFAULT NULL,`Virginia` int(3) DEFAULT NULL,`Washington` int(3) DEFAULT NULL,`West_Virginia` int(3) DEFAULT NULL,`Wisconsin` int(3) DEFAULT NULL,`Wyoming` int(2) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
##cur.execute(createTable2)

#....Checks to see if this script is being called by a different script....#
def init():
    if __name__ == '__main__':
        print "populateMySQL.py being called natively"
        addRows()

    else:
        print "populateMySQL.py being called from elsewhere"


#....Check if a row for today exists already in each table - if not, adds one....#
def addRows():
    # Check if row already exists in truck table
    try:
        todayExistTruck = "SELECT EXISTS(SELECT * FROM truck_data WHERE date = '" + datestamp + "')"
        cur.execute(todayExistTruck)
        row = cur.fetchall()
        if row[0][0] > 0:
            print "Row exists for today in truck table"
        else:
            print "Row doesn't exist for today in truck table. Adding now..."
            addRow = "INSERT INTO truck_data (date) VALUES ('" + datestamp + "') "
            cur.execute(addRow)
            print "Done adding row for today in truck table"

    except MySQLdb.Error, e:
        print "There was a DB error"

    # Check if row already exists in warehouse table
    try:
        todayExistWarehouse = "SELECT EXISTS(SELECT * FROM warehouse_data WHERE date = '" + datestamp + "')"
        cur.execute(todayExistWarehouse)
        row = cur.fetchall()
        if row[0][0] > 0:
            print "Row exists for today in warehouse table"
        else:
            print "Row doesn't exist for today in warehouse table. Adding now..."
            addRow = "INSERT INTO warehouse_data (date) VALUES ('" + datestamp + "') "
            cur.execute(addRow)
            print "Done adding row for today in warehouse table"

    except MySQLdb.Error, e:
        print "There was a DB error"

    populateDB_truck()


#....Populate the truck table - go through once and catch errors, then go through errors...#
def populateDB_truck():
    i = 0
    failed = []
    while i < len(states):
        statelower = states[i].lower()
        yql_query = 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20xml%20where%20url%20%3D%20%22api.careerbuilder.com%2Fv1%2Fjobsearch%3FDeveloperKey%3D#####%26location%3D' + statelower +'%26keywords%3Dtruck%2520driver%22&diagnostics=true'
        result = urllib2.urlopen(yql_query)
        try:
            tree = ET.parse(result)
            root = tree.getroot()
            jobs = root[1][0][4].text
            print states[i] + ": " + jobs + " jobs"
            insertvaluessql = "UPDATE truck_data SET " + statesFull[i] + " = " + jobs + " WHERE date = '" + datestamp + "'"
            cur.execute(insertvaluessql)
        except IndexError, e:
            print "Failed to insert data for " + statesFull[i] + " - moved to failed list"
            failed.append(states[i])

        i += 1

    # Check ones with errors
    print "Attempting to fix states with errors..."
    j = 0
    while j < len(failed):
        statelower = failed[j].lower()
        yql_query = 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20xml%20where%20url%20%3D%20%22api.careerbuilder.com%2Fv1%2Fjobsearch%3FDeveloperKey%3D#####%26location%3D' + statelower +'%26keywords%3Dtruck%2520driver%22&diagnostics=true'
        result = urllib2.urlopen(yql_query)
        try:
            tree = ET.parse(result)
            root = tree.getroot()
            jobs = root[1][0][4].text
            print failed[j] + ": " + jobs + " jobs"
            insertvaluessql = "UPDATE truck_data SET " + statesFull[j] + " = " + jobs + " WHERE date = '" + datestamp + "'"
            cur.execute(insertvaluessql)
        except IndexError, e:
            print "Failed to insert data for " + statesFull[j] + " - moved to failed list"
            failed.append(failed[j])

        j += 1

    print "Done with truck jobs"

    populateDB_warehouse()


#....Populate warehouse table....#
def populateDB_warehouse():
    print "Starting warehouse jobs..."
    i = 0
    failed = []
    while i < len(states):
        statelower = states[i].lower()
        yql_query = 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20xml%20where%20url%20%3D%20%22api.careerbuilder.com%2Fv1%2Fjobsearch%3FDeveloperKey%3D#####%26location%3D'+statelower+'%26keywords%3Dwarehousing%26excludekeywords%3Ddata%22&diagnostics=true'
        result = urllib2.urlopen(yql_query)
        try:
            tree = ET.parse(result)
            root = tree.getroot()
            jobs = root[1][0][4].text
            print states[i] + ": " + jobs + " jobs"
            insertvaluessql = "UPDATE warehouse_data SET " + statesFull[i] + " = " + jobs + " WHERE date = '" + datestamp + "'" 
            cur.execute(insertvaluessql)
        except IndexError, e:
            print "Failed to insert data for " + statesFull[i] + " - moved to failed list"
            failed.append(states[i])

        i += 1

    # Check ones with errors
    print "Attempting to fix states with errors..."
    j = 0
    while j < len(failed):
        statelower = failed[j].lower()
        yql_query = 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20xml%20where%20url%20%3D%20%22api.careerbuilder.com%2Fv1%2Fjobsearch%3FDeveloperKey%3D#####%26location%3D' + statelower +'%26keywords%3Dtruck%2520driver%22&diagnostics=true'
        result = urllib2.urlopen(yql_query)
        try:
            tree = ET.parse(result)
            root = tree.getroot()
            jobs = root[1][0][4].text
            print failed[j] + ": " + jobs + " jobs"
            insertvaluessql = "UPDATE warehouse_data SET " + statesFull[j] + " = " + jobs + " WHERE date = '" + datestamp + "'"
            cur.execute(insertvaluessql)
        except IndexError, e:
            print "Failed to insert data for " + statesFull[j] + " - moved to failed list"
            failed.append(failed[j])

        j += 1

    print "Done with warehousing jobs"

init()