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

# Connect to CFIRE MySQL DB
db = MySQLdb.connect(host="mysql-general.cae.wisc.edu",
                     user="jczaplewski",
                     passwd="mafcMAP12",
                     db="map_data")

# Create cursor to execture queries against the DB
cur = db.cursor() 


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
        todayExistTruck = "SELECT EXISTS(SELECT * FROM truck_data_revised WHERE date = '" + datestamp + "')"
        cur.execute(todayExistTruck)
        row = cur.fetchall()
        if row[0][0] > 0:
            print "Row exists for today in truck table"
        else:
            print "Row doesn't exist for today in truck table. Adding now..."
            addRow = "INSERT INTO truck_data_revised (date) VALUES ('" + datestamp + "') "
            cur.execute(addRow)
            print "Done adding row for today in truck table"

    except MySQLdb.Error, e:
        print "There was a DB error"

    # Check if row already exists in warehouse table
    try:
        todayExistWarehouse = "SELECT EXISTS(SELECT * FROM warehouse_data_revised WHERE date = '" + datestamp + "')"
        cur.execute(todayExistWarehouse)
        row = cur.fetchall()
        if row[0][0] > 0:
            print "Row exists for today in warehouse table"
        else:
            print "Row doesn't exist for today in warehouse table. Adding now..."
            addRow = "INSERT INTO warehouse_data_revised (date) VALUES ('" + datestamp + "') "
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
        yql_query = 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20xml%20where%20url%20%3D%20%22api.careerbuilder.com%2Fv1%2Fjobsearch%3FDeveloperKey%3DWDTZ5QX77H4WN1RXC33V%26location%3D' + statelower +'%26keywords%3Dtruck%2520driver%22&diagnostics=true'
        result = urllib2.urlopen(yql_query)
        try:
            tree = ET.parse(result)
            root = tree.getroot()
            jobs = root[1][0][4].text
            print states[i] + ": " + jobs + " jobs"
            insertvaluessql = "UPDATE truck_data_revised SET " + statesFull[i] + " = " + jobs + " WHERE date = '" + datestamp + "'"
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
        yql_query = 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20xml%20where%20url%20%3D%20%22api.careerbuilder.com%2Fv1%2Fjobsearch%3FDeveloperKey%3DWDTZ5QX77H4WN1RXC33V%26location%3D' + statelower +'%26keywords%3Dtruck%2520driver%22&diagnostics=true'
        result = urllib2.urlopen(yql_query)
        try:
            tree = ET.parse(result)
            root = tree.getroot()
            jobs = root[1][0][4].text
            print failed[j] + ": " + jobs + " jobs"
            insertvaluessql = "UPDATE truck_data_revised SET " + statesFull[j] + " = " + jobs + " WHERE date = '" + datestamp + "'"
            cur.execute(insertvaluessql)
        except IndexError, e:
            print "Failed to insert data for " + statesFull[j] + " - moved to failed list"
            failed.append(failed[j])

        j += 1

    populateDB_warehouse()


#....Populate warehouse table....#
def populateDB_warehouse():
    print "Starting warehouse jobs..."
    i = 0
    failed = []
    while i < len(states):
        statelower = states[i].lower()
        yql_query = 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20xml%20where%20url%20%3D%20%22api.careerbuilder.com%2Fv1%2Fjobsearch%3FDeveloperKey%3DWDTZ5QX77H4WN1RXC33V%26location%3D'+statelower+'%26keywords%3Dwarehousing%26excludekeywords%3Ddata%22&diagnostics=true'
        result = urllib2.urlopen(yql_query)
        try:
            tree = ET.parse(result)
            root = tree.getroot()
            jobs = root[1][0][4].text
            print states[i] + ": " + jobs + " jobs"
            insertvaluessql = "UPDATE warehouse_data_revised SET " + statesFull[i] + " = " + jobs + " WHERE date = '" + datestamp + "'" 
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
        yql_query = 'http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20xml%20where%20url%20%3D%20%22api.careerbuilder.com%2Fv1%2Fjobsearch%3FDeveloperKey%3DWDTZ5QX77H4WN1RXC33V%26location%3D' + statelower +'%26keywords%3Dtruck%2520driver%22&diagnostics=true'
        result = urllib2.urlopen(yql_query)
        try:
            tree = ET.parse(result)
            root = tree.getroot()
            jobs = root[1][0][4].text
            print failed[j] + ": " + jobs + " jobs"
            insertvaluessql = "UPDATE warehouse_data_revised SET " + statesFull[j] + " = " + jobs + " WHERE date = '" + datestamp + "'"
            cur.execute(insertvaluessql)
        except IndexError, e:
            print "Failed to insert data for " + statesFull[j] + " - moved to failed list"
            failed.append(failed[j])

        j += 1
        
    checkit()


#....Check to make sure each state has a value for today....#
def checkit():
    try:
        checksql = "SELECT * FROM warehouse_data_revised WHERE date = '" + datestamp + "'"
        cur.execute(checksql)
        result = cur.fetchall()
        print result[0][50]
        if result[0][50] == None :
            print "Wyoming is fucked up. Let's try again..."
            populateDB_truck()
        else:
            print "Error Check complete"
    except MySQLdb.Error, e:
        print "MySQL error"

init()