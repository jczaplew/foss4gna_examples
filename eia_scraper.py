## Script to scrape all the powerplant level data from the EIA database
## and insert it into a MySQL database

## Requires MySQLdb, which can be downloaded here: https://pypi.python.org/pypi/MySQL-python/1.2.4

## John J Czaplewski | jczaplew@gmail.com | Februrary, 2013


import urllib2
import json
import MySQLdb

#Connect to MySQL
db = MySQLdb.connect(host="#####", user="#####", passwd="######", db="#####")

cur = db.cursor() 

#Create a table to store the data
createTable = "CREATE TABLE eia ()"

cur.execute(createTable);

#First query finds the IDs of all the available powerplants
request1 = urllib2.urlopen('http://api.eia.gov/category/?api_key=###your_api_key###&category_id=1018')

powerplants = json.load(request1)

#Now we loop through the results and package all of the IDs into a list for use later
powerplantIds = []
i = 0
while (i < len(powerplants["category"]["childcategories"])):
    powerplantIds.append(powerplants["category"]["childcategories"][i]["category_id"])
    i+=1

# Start looping through the list of powerplants that was just created...
j = 0
while (j < len(powerplantIds)):
    #Start navigating down the JSON...for each powerplant, find all available categories of data
    request2 = urllib2.urlopen('http://api.eia.gov/category/?api_key=###your_api_key###&category_id=' + str(powerplantIds[j]))
    powerplantNetGen = json.load(request2)
    x = 0
    #Loop through all available categories for each powerplant, and grab only the ones that are annual data for a specific type of fuel source
    keepers = []
    while (x < len(powerplantNetGen["category"]["childseries"])):
        name = powerplantNetGen["category"]["childseries"][x]["name"]
        name = name.split(" : ")
        if name[3] != "All Primemovers" and powerplantNetGen["category"]["childseries"][x]["f"] == "A" :
            keepers.append(powerplantNetGen["category"]["childseries"][x]["series_id"])
        x += 1

    #Now that we know all the fuel types for a powerplant, we're going to request that type's most specific data
    y = 0
    while (y < len(keepers)) :
        request3 = urllib2.urlopen('http://api.eia.gov/series/?series_id=' + keepers[y] + '&api_key=###your_api_key###')
        stationData = json.load(request3)
        name = stationData["series"][0]["name"]
        nameSanitized = name.replace("'", "")
        nameSanitized = nameSanitized.split(" : ")

        #We have the data, just have to clean it up before inserting into the database
        years = ""
        yearData = ""
        z = 0
        while z < len(stationData["series"][0]["data"]):
            if z == len(stationData["series"][0]["data"]) - 1:
                years = years + "data_" + stationData["series"][0]["data"][z][0]
                yearData = yearData + "'" + stationData["series"][0]["data"][z][1] + "'"
            else:
                years = years + "data_" +  stationData["series"][0]["data"][z][0] + ", "
                yearData = yearData + "'" + stationData["series"][0]["data"][z][1] + "'" + ", "
            z += 1

        # Finally insert the data into the database
        sql = "INSERT INTO eia (name, fuel, generator, series_id, lat, lon, source, " + years + ") VALUES ('" + nameSanitized[1] + "', '" + nameSanitized[2] + "', '" + nameSanitized[3] + "', '" + stationData["series"][0]["series_id"] + "', '" + stationData["series"][0]["lat"] + "', '" + stationData["series"][0]["lon"] + "', '" + stationData["series"][0]["source"] + "', " + yearData + ")"
        cur.execute(sql)
        print "Inserted a row"
        y += 1

    #Now do it again, and again
    print "Done with " + str(j) + " of " + str(len(powerplantIds))
    j += 1

print "Finished!"