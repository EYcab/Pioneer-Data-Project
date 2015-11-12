__author__ = 'Chenxi'

import sqlite3 as lite
import sys

# This part I query the data from the factor_db table I created and use array and sets exclusion logic
# to get the result array and output its elements to the file as required in the question

def factors_collections():
    global result, all, e
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    try:
        cur.execute("SELECT distinct(station),count(id) "
                    "FROM factors_db "
                    "where max_Temp !=-9999 and min_Temp !=-9999 and precipitation = -9999 "
                    "GROUP BY station "
                    )
    except lite.Error, e:
        if con:
            con.rollback()
        print "the factor_db was not built,yet"
        print "Error %s:" % e.args[0]
        sys.exit(1)
    finally:
        if con:
            result = cur.fetchall()
            cur.execute("SELECT distinct(station) from factors_db")
            all = cur.fetchall()
            # print all
            con.commit()
            con.close()


def get_arr_result():
    all_name = []
    all_result = []
    for e in all:
        all_name.append(e[0])
    for item in result:
        all_result.append(item[0])
    rest = set(all_name).difference(set(all_result))
    for item in rest:
        result.append((item, 0))
    result.sort()


if __name__=="__main__":
    factors_collections()
    get_arr_result()
    #print result
    outFile = open("C://Users//Chenxi//Desktop//examContent//coding-data-exam//answers//MissingPrcpData.out", "w")

    for s in result:
        ln = s[0] + "\t" + str(s[1]) + "\n"
        outFile.write(ln)
    outFile.close()

