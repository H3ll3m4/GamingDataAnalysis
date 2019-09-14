import csv
#import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
from sqlite3 import Error
# https://elitedatascience.com/python-seaborn-tutorial
# Seaborn for plotting and styling
#import seaborn as sns

#https://pythonprogramming.net/reading-csv-files-python-3/
def ExploringData(name):
    with open('SimilarWeb/similarweb-webtraffic-20190719.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        sites = []
        countries = []
        years = []
        desktop_visit_tab = []
        desktop_pages_per_visit_tab = []

        for row in readCSV:

            site = row[0]
            if site == name:
                country = row[1]
                year = row[2]
                desktop_visit = row[5]
                desktop_pages_per_visit = row[6]

                sites.append(site)
                countries.append(country)
                years.append(year)
                desktop_visit_tab.append(desktop_visit)
                desktop_pages_per_visit_tab.append(desktop_pages_per_visit)

        print(sites)
        print(countries)
        print(desktop_visit_tab)
        print(desktop_pages_per_visit_tab)
        #gc.collect()


def ExploringDataPanda():
# pd=csv.reader("select * from stock_prices", con=db).set_index('id')
    #df.read_csv('SimilarWeb/similarweb-webtraffic-20190719.csv', delimiter=',')
    df = pd.read_csv('SimilarWeb/similarweb-webtraffic-20190719.csv', delimiter=',', names = ['site',	'country',	'year',	'month',	'day',	'desktop_visits',	'desktop_pages_per_visit'	,'desktop_visit_duration',	'desktop_bounce_rate',	'desktop_page_views'	,'mobile_visits',	'mobile_pages_per_visit',	'mobile_visit_duration',	'mobile_bounce_rate', 	'mobile_page_views',	'total_visits'	,'total_pages_per_visits'	,'total_visit_duration'	,'total_bounce_rate',	'total_page_views'])
    df.head()
    df.tail()
    #df = pd.drop('year', 1)
    #'site',	'country',	'year',	'month',	'day',	'desktop_visits',	'desktop_pages_per_visit'	,'desktop_visit_duration',	'desktop_bounce_rate',	'desktop_page_views'	,'mobile_visits',	'mobile_pages_per_visit',	'mobile_visit_duration',	'mobile_bounce_rate', 	'mobile_page_views',	'total_visits'	,'total_pages_per_visits'	,'total_visit_duration'	,'total_bounce_rate',	'total_page_views'
    #df.drop(df.columns[[1, 69]], axis=1, inplace=True)
    df_activision = df[df['sites'].str.contains('activision.com')]
    df_rockstargames = df[df['sites'].str.contains('rockstargames.com')]
    df_ea = df[df['sites'].str.contains('ea.com')]
    df_blizzard = df[df['sites'].str.contains('blizzard.com')]
    df_bioware = df[df['sites'].str.contains('bioware.com')]
    plt.subplots()
    #barplot


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


#https://www.sqlitetutorial.net/sqlite-python/sqlite-python-select/
def ExploringDB():
    #if __name__ == '__main__':
    conn = create_connection('altdata-hackathon/data/hackathon.db')
    with conn:
        cur = conn.cursor()
        cur.execute(".tables")
        #cur.execute("SELECT * FROM tasks WHERE")

        rows = cur.fetchall()

        for row in rows:
            print(row)



def ExploringDataFundamental():
    with open('OpenData/Fundamentals/fundamentals_Electronic Arts Inc..csv') as csvfile:
    #with open('OpenData/Fundamentals/fundamentals_Ubisoft.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        totalRevenueTab = []
        costOfRevenueTab = []

        for row in readCSV:
            totalRevenue = row[2]
            costOfRevenue = row[3]


            totalRevenueTab.append(totalRevenue)
            costOfRevenueTab.append(costOfRevenue)

        print(totalRevenue)
        print(costOfRevenue)




#ExploringData('activision.com') # too slow!
#ExploringDataPanda()
ExploringDataFundamental()