__author__ = 'Chenxi'
import sqlite3 as lite

#Here, I query each individual column of data from the aggregate_db to include as many available data as possible
#as we only care about the correlation between x and y variables
#Also, I have used lists as the agent to transfer data from each individual factor collection to the output result file
#Finally, I built up a halfWay_db table to hold my current data storage in problem2 (same as the YearlyAverages.out)
#This would help me solve problem4
def procedures_for_each_parameter(factor):
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    cur.execute(
        "SELECT station,year,avg("+factor+") "
        "FROM aggregate_db "
        "where "+factor+" !=-9999 "
        "group by station,year"
    )

    result = cur.fetchall()
    print result
    return result

def parameter_arr(list_input,factor):
    if factor !=0:
        arr = []
        for item in list_input:
            m = round(item[2]/factor,2)
            arr.append(m)
        return arr

def halfWay_db_construction():
    # try to inject the halfWay_db
    con = lite.connect('FactorsData.db')
    cur = con.cursor()
    try:
        cur.execute('''CREATE TABLE halfWay_db
                 (station TEXT, year INT, avg_max_Temp REAL,
                  avg_min_Temp REAL, avg_precipitation REAL)''')
        cur.executemany('INSERT INTO halfWay_db VALUES (?,?,?,?,?)', result_list)
        print "It just got halfWay_db built"
    except lite.Error, e:
        if con:
            con.rollback()
        print "halfWay_db already exists"
    finally:
        if con:
            # cur.execute("SELECT * FROM halfWay_db")
            # row = cur.fetchall()
            # print row
            con.commit()
            con.close()

if __name__=="__main__":
    max_Temp_result=procedures_for_each_parameter("max_Temp")
    min_Temp_result=procedures_for_each_parameter("min_Temp")
    prec_result=procedures_for_each_parameter("precipitation")

    max_Temp_arr=parameter_arr(max_Temp_result,10)
    min_Temp_arr=parameter_arr(min_Temp_result,10)
    prec_arr = parameter_arr(prec_result,100)

    result_list = []
    for item in max_Temp_result:
        result_list.append((item[0],item[1],max_Temp_arr.pop(0),min_Temp_arr.pop(0),prec_arr.pop(0)))
    #print result_list
    outFile = open("C://Users//Chenxi//Desktop//examContent//coding-data-exam//answers//YearlyAverages.out", "w")
    for s in result_list:
        ln = s[0] + "\t" + str(s[1]) +  "\t" + str(s[2]) +"\t" + str(s[3])+"\t" + str(s[4])+"\n"
        outFile.write(ln)
    outFile.close()
    del max_Temp_arr[:]
    del min_Temp_arr[:]
    del prec_arr[:]
    halfWay_db_construction()
    del result_list[:]
