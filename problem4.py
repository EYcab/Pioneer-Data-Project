__author__ = 'Chenxi'
import sqlite3 as lite
import numpy as np
import copy
from collections import Counter
#create a grain_yield_db table to store the grain yield data
#map all the data from the factors and yield data with station filename to tables
#and perform correlation calculation via numpy package(please install numpy if you don't have)
#and map all the individual file result to the correlation_summary table
#Finally, we output the data to Correlations.out


def build_db(parameter_list):
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    try:
        cur.execute('''CREATE TABLE grain_yield_db
                 (year INT, yield INT)''')
        cur.executemany('INSERT INTO grain_yield_db VALUES (?,?)', parameter_list)

        cur.executescript("""
        DROP TABLE IF EXISTS grain_yield_db;
        """)

    except lite.Error,e:
        if con:
            con.rollback()
        print "The grain_yield_db already exists"

    finally:
        if con:
            cur.execute("SELECT * FROM grain_yield_db")
            row = cur.fetchall()
            #print row
            con.commit()
            con.close()

def correlation_construction():
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    try:
        cur.execute('''CREATE TABLE correlation_db AS
                    SELECT H.station, H.year, avg_max_Temp,avg_min_Temp,
                    avg_precipitation, yield
                    FROM halfWay_db AS H, grain_yield_db AS G
                    WHERE H.year = G.year
                    ORDER BY H.station
                    ''')

    # cur.executescript("""
    # DROP TABLE IF EXISTS correlation_db;
    # """)
    except lite.Error,e:
        if con:
            con.rollback()
        print "The correlation_db already exists"

    finally:
        if con:
            cur.execute("SELECT * FROM correlation_db")
            row = cur.fetchall()
            print row
            con.commit()
            con.close()

#pair up factors and yield variables with station individually via database table
def get_keys(element):
    """

    :rtype : object
    """
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    try:
        cur.execute('''CREATE TABLE '''+element+'''_db AS
                SELECT station,'''+element+''',yield
                FROM correlation_db
                ORDER BY station
                ''')
    # cur.executescript("""
    # DROP TABLE IF EXISTS """+element+"""_db;
    # """)
    #
    except lite.Error,e:
        if con:
            con.rollback()
        print "The database already exists"


    finally:
        if con:
            cur.execute("SELECT * FROM "+element+"_db")
            row = cur.fetchall()
            con.commit()
            con.close()
            return row

#arr is the target calculation original arr, id is the name of the station file
#perform data mapping and pearson correlation calculation via arrays and numpy
#output contains only station and correlation data
def get_result(arr,id):
    arr.append("end")
    catalog = []
    #print "check",arr
    for e in id:
        key1 = []
        key2 = []
        while arr:
            if e != arr[0][0]:
                e1=copy.copy(key1)
                e2=copy.copy(key2)
                catalog.append((e,e1,e2))
                #print catalog
                break
            key1.append(arr[0][1])
            key2.append(arr[0][2])
            arr.pop(0)
        del key1[:]
        del key2[:]
    print "here",catalog[-1]
    #return catalog
    output = []
    while catalog:
        d = np.corrcoef(catalog[0][1],catalog[0][2])
        output.append((catalog[0][0],round(d[0][1],2)))
        catalog.pop(0)
    return output

# get a station list
def get_station(arr):
    station = []
    for item in arr:
        station.append(item[0])
    s = Counter(station).keys()
    s.sort()
    del station[:]
    return s

# the whole correlation function
def correlation_operation(str_var):
    avg_max_arr = get_keys(str_var)
    station_arr = get_station(avg_max_arr)
    sub_correlation = get_result(avg_max_arr, station_arr)
    return sub_correlation

# input all the correlation data pair (station, correlation) to its individual correlation table
def correlation_sub_map(arr_str,correlation_list):
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    try:
        cur.execute('''CREATE TABLE '''+arr_str+'''_db
                     (station TEXT, correlation INT)''')
        cur.executemany('INSERT INTO '+arr_str+'_db VALUES (?,?)', correlation_list)

        # cur.executescript("""
        # DROP TABLE IF EXISTS """+arr_str+"""_db;
        # """)

    except lite.Error,e:
        if con:
            con.rollback()
        print "the db already exists"

    finally:
        if con:
            cur.execute("SELECT * FROM "+arr_str+"_db")
            row = cur.fetchall()
            print row
            con.commit()
            con.close()

# create a combined table to hold all the correlation data tables
def correlation_map_to_db():
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    try:
        cur.execute('''CREATE TABLE correlation_summary_db AS
                        SELECT HT.station, HT.correlation AS avg_max_Temp,
                            LT.correlation AS avg_min_Temp,
                            AP.correlation AS avg_prec
                        FROM correlation_avg_max_Temp_db AS HT, correlation_avg_min_Temp_db AS LT,
                            correlation_avg_prec_db AS AP
                        WHERE HT.station = LT.station and HT.station = AP.station
                        ORDER BY HT.station
                    ''')
        # cur.executescript("""
        # DROP TABLE IF EXISTS """+arr_str+"""_db;
        # """)

    except lite.Error,e:
        if con:
            con.rollback()
        print "the db already exists"

    finally:
        if con:
            cur.execute("SELECT * FROM correlation_summary_db")
            row = cur.fetchall()
            con.commit()
            con.close()
            return row



if __name__=="__main__":
    year_grain_list = []
    f = open("C://Users//Chenxi//Desktop//examContent//coding-data-exam//yld_data//US_corn_grain_yield.txt", 'rb')
    for line in f:
        year,grain_yield = line.strip('\r\n').split("\t")
        year_grain_list.append((year,grain_yield))
    build_db(year_grain_list)
    f.close()
    #construct a correlation data table to all the relevant data
    correlation_construction()
    #perform correlation calculations on each x-y-factor-pairs
    avg_max_Temp_correlation=correlation_operation("avg_max_Temp")
    avg_min_Temp_correlation=correlation_operation("avg_min_Temp")
    avg_prec_correlation=correlation_operation("avg_precipitation")
    # print avg_max_Temp_correlation
    # print avg_min_Temp_correlation
    # print avg_prec_correlation

    #build 3 individual sub-correlation tables
    correlation_sub_map("correlation_avg_max_Temp",avg_max_Temp_correlation)
    correlation_sub_map("correlation_avg_min_Temp",avg_min_Temp_correlation)
    correlation_sub_map("correlation_avg_prec",avg_prec_correlation)
    # create a single data table to hold all the sub-correlation tables via mapping
    correlation_list=correlation_map_to_db()

    outFile = open("C://Users//Chenxi//Desktop//examContent//coding-data-exam//answers//Correlations.out", "w")
    for c in correlation_list:
        ln = c[0] + "\t" + str(c[1]) +  "\t" + str(c[2]) +"\t" + str(c[3])+"\n"
        outFile.write(ln)
    outFile.close()







