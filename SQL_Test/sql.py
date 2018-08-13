__author__ = 'fabersh'
# -*- coding: utf-8 -*-
import sqlite3, csv, sys, os
from operator import itemgetter
import numpy as np
import statistics


#Code to create a SQLITE table to hold GBPEUR time series. Eventually both currencies can
#be handled in one table after refactoring to save the amount that needs to be maintained.
def create_table_GBPEUR():
    db = sqlite3.connect('db.sqlite3')
    cursor = db.cursor()
    cursor.execute('''
        CREATE TABLE GBPEUR(
            valuation_date DATE PRIMARY KEY,
            underlying TEXT,
            mid REAL)''')
    db.commit()
    db.close()

#Inserts time GBPEUR time series into a table in SQLITE.
#Can be normalized in next iteration with GBPUSD
def load_GBPEUR():
    db = sqlite3.connect('db.sqlite3')
    cursor = db.cursor()
    with open('Documentation/GBPEUR.csv', 'rU') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        to_db = [( i['Valuation_Date'], i['Underlying'], i['Mid']) for i in reader]
        cursor.executemany('''
            INSERT INTO GBPEUR(
            valuation_date,
            underlying,
            mid)
            VALUES (?, ?, ?);''', to_db)
        db.commit()
        db.close()


#Next iteration can have one parameter that receives the currency pair.
#This will let us get rid of unnecessary code and easier maintenance.
def create_table_GBPUSD():
    db = sqlite3.connect('db.sqlite3')
    cursor = db.cursor()
    cursor.execute('''
        CREATE TABLE GBPUSD(
            valuation_date DATE PRIMARY KEY,
            underlying TEXT,
            mid REAL)''')
    db.commit()
    db.close()



def load_GBPUSD():
    db = sqlite3.connect('db.sqlite3')
    cursor = db.cursor()
    with open('Documentation/GBPUSD.csv', 'rU') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        to_db = [( i['Valuation_Date'], i['Underlying'], i['Mid']) for i in reader]
        cursor.executemany('''
            INSERT INTO GBPUSD(
            valuation_date,
            underlying,
            mid)
            VALUES (?, ?, ?);''', to_db)
        db.commit()
        db.close()


#Code to retrieve the years for rolling calculations 1Y, 2Y, 3Y
#SELECT substr(valuation_date, 7,10) as year FROM 'GBPEUR' group by year;
def get_rolling_years(currency_pair):
    query_strings = str
    query_strings = 'SELECT substr(valuation_date, 7,10) as year FROM ' + currency_pair + ' group by year'
    return query_strings



# SQL statement to fecth data from the database. Ideally the SQL code should not
# be exposed in the main modules for security reasons.
def build_fetch_statement(currency_pair):
    query_strings = str
    query_strings = 'SELECT valuation_date, underlying, mid FROM '+ currency_pair +' ORDER BY valuation_date'
    return query_strings



# Save results in an object with the years needed for 1Y, 2y, 3Y
# i.e.  Daily_Returns, Average, Standard_Deviation, Covariance, Correlation
def generate_resultset(query):
    with sqlite3.connect('db.sqlite3') as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results

#Main
#**************************************************************************************************
#This is the testing area

# create_table_GBPUSD()
# load_GBPUSD()
# print("done with GBPUSD")

#Testing the query to get the rolling years
print(get_rolling_years('GBPEUR'))
x = generate_resultset(get_rolling_years('GBPEUR'))
print (x[1])
print (x[2])
print (x[3])


#Testing the fetch statement to retrieve the time series for BGPEUR
print(build_fetch_statement('GBPEUR'))

y = generate_resultset(build_fetch_statement('GBPEUR'))


#Testing the array function needed to pass to the statistical methods in numpy and statistics methods (stats.py)
BGPEUR_array = np.asarray(y)
for i in range(0, 1000):
    print (BGPEUR_array[i][2])


# import statistics
# import numpy
# samplex = BGPEUR_array
# sampley = [1.446,1.453,1.446,1.448,1.449,1.452,1.446]


# def average(sample):
#     x = statistics.mean(samplex)
#     return x
#
# def standard_dev(samplex):
#     x = statistics.stdev(samplex, xbar = None)
#     return x
#
# print(average(samplex))


#******************************************************************************************************************