__author__ = 'Chenxi'
import sqlite3
import glob,os

# Use sqlite3
#This part is to build up data tables aggregate_db and factors_db in database FactorsData.db
#Of course, we read in the data files from wx_data.txt file

def aggregate_db_construction():
    con = sqlite3.connect('FactorsData.db')
    cur = con.cursor()
    try:
        # create new db and make connection
        # create table
        cur.execute('''CREATE TABLE aggregate_db
                 (id INT,station TEXT, max_Temp INT,
                  min_Temp INT,precipitation INT,
                  year INT)''')
        cur.executemany('INSERT INTO aggregate_db VALUES (?,?,?,?,?,?)', aggre_list)
        print "Just Got build"
    except sqlite3.Error, e:
        if con:
            con.rollback()
        print "Error %s:" % e.args[0]

    finally:
        if con:
            # close connection
            cur.execute("SELECT * FROM aggregate_db")
            row = cur.fetchone()
            print row
            con.commit()
            con.close()
def factors_db_construction():
    con = sqlite3.connect('FactorsData.db')
    cur = con.cursor()
    try:
        # create new db and make connection
        # create table
        cur.execute('''CREATE TABLE factors_db
                     (id INT,station TEXT, max_Temp INT,
                      min_Temp INT,precipitation INT)''')
        cur.executemany('INSERT INTO factors_db VALUES (?,?,?,?,?)', factors_list)
        print "Just Got build"
    except sqlite3.Error, e:
        if con:
            con.rollback()
        print "Error %s:" % e.args[0]
    finally:
        if con:
            # close connection
            cur.execute("SELECT * FROM factors_db")
            row = cur.fetchone()
            print row
            con.commit()
            con.close()



if __name__=="__main__":
    factors_list = []
    aggre_list = []
    file_name_arr = []
    file_folder = []
    path = "C://Users//Chenxi//Desktop//examContent//coding-data-exam//wx_data//*"
    for name in glob.glob(path):
        file_name_arr.append(os.path.basename(name))

    for file_path in glob.glob(path):
        file_folder.append(file_path)
    i=0

    while file_folder:
        station = file_name_arr.pop(0)
        f = open(file_folder.pop(0), 'rb')

        for line in f:
            date, maxTemp, minTemp, prec =line.strip('\r\n').split("\t")
            year,month,day = date[:4],date[4:6],date[6:8]
            factors_list.append((i,station,maxTemp,minTemp,prec))
            aggre_list.append((i,station,maxTemp,minTemp,prec,year))
            i+=1
        f.close()
    del file_name_arr[:]
    del file_folder[:]
    aggregate_db_construction()
    factors_db_construction()
    del factors_list[:]
    del aggre_list[:]


