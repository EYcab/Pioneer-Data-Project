__author__ = 'Chenxi'
import sqlite3 as lite
import sys
import csv
from collections import Counter

# create sub tables to hold Max values based on station during the period and create a table to hold
# the year, station and the max values order by years
# use the get_counts function to count the maximum value for each year
# print all those by mapping to the answer files from the storage arrays where collecting the year counts
# output data to a csv file and also use excel to produce a histogram

def get_max_parameter(factor):
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    try:
        cur.execute('''CREATE TABLE sub_'''+factor+'''_db AS
                 SELECT station,MAX('''+factor+''') AS sub
                 FROM halfWay_db
                 GROUP BY station
                ''')
        cur.execute('''CREATE TABLE max_'''+factor+'''_db AS
                SELECT H.year,H.station, H.'''+factor+'''
                FROM halfWay_db AS H,sub_'''+factor+'''_db AS S
                WHERE H.'''+factor+'''=S.sub and H.station = S.station
                ORDER BY H.year
               ''')
    # cur.executescript("""
    # DROP TABLE IF EXISTS sub_avg_max_Temp_db;
    # """)
    # cur.executescript("""
    # DROP TABLE IF EXISTS max_avg_max_Temp_db;
    # """)
    # cur.executescript("""
    # DROP TABLE IF EXISTS sub_avg_min_Temp_db;
    # """)

    except lite.Error,e:
        if con:
            con.rollback()
        print "the db already exists"

    finally:
        if con:
            cur.execute("SELECT * FROM max_"+factor+"_db")
            row = cur.fetchall()
            con.commit()
            con.close()
            return row

def get_counts(in_list):
    out_arr = []
    value = []
    for item in in_list:
        out_arr.append(item[0])
    key= Counter(out_arr).keys()
    out_arr = Counter(out_arr).items()
    i = 0
    for num in xrange(1985,2015):
        if key[i]!=num:
            out_arr.append((num,0))
        else:
            i+=1
    out_arr.sort()
    for item in out_arr:
        value.append(item[1])
    # print value,"here",len(value)
    return value

if __name__=="__main__":
    max_avg_max_Temp=get_max_parameter("avg_max_Temp")
    max_avg_min_Temp=get_max_parameter("avg_min_Temp")
    max_avg_prec=get_max_parameter("avg_precipitation")
    value_max = get_counts(max_avg_max_Temp)
    value_min = get_counts(max_avg_min_Temp)
    value_prec = get_counts(max_avg_prec)

    outFile = open("C://Users//Chenxi//Desktop//examContent//coding-data-exam//answers//YearHistogram.out", "w")
    with open('C://Users//Chenxi//Desktop//examContent//coding-data-exam//answers//histogram.csv', 'wb') as csvfile:
        fieldnames = ['Year', 'Peak counts annual average annual max-temperature',
                      'Peak counts for annual average annual min-temperature',
                      'Peak counts annual average annual precipitation']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in xrange(1985,2015):
            a = value_max.pop(0)
            b = value_min.pop(0)
            c = value_prec.pop(0)
            ln = str(i) + "\t" + str(a) +  "\t" + str(b) +"\t" + str(c)+"\n"
            writer.writerow({'Year':i ,'Peak counts annual average annual max-temperature':a,
                             'Peak counts for annual average annual min-temperature': b,
                             'Peak counts annual average annual precipitation':c}),
            outFile.write(ln)
    outFile.close()
    csvfile.close()

