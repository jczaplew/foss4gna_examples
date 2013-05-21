# FOSS4G-NA 2013 Presentation Scripts

These are the two scripts used for data collection in the examples used for my presentation at the FOSS4G-NA conference in Minneapolis, MN on May 22, 2013. If you'd like to see the original slides, [find them here.](http://johnjcz.com/presentations/foss4g2013).

**eia_scraper.py** - scrape all the powerplant level data from the EIA database and insert it into a MySQL database. Requires an [EIA API key](http://www.eia.gov/beta/api/register.cfm)

**truck_drivers.py** - gathers the number of truck driver and warehousing jobs in each state and inserts them into a MySQL database. Requires a [Careerbuilder.com API key](http://api.careerbuilder.com/RequestDevKey.aspx)

Both scripts require the Python module [MySQLdb](https://pypi.python.org/pypi/MySQL-python/)