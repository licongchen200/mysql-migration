import mysql.connector
import codecs
import json


srcDB = 'mydb'
destDB = 'mydb'

mydb_src = mysql.connector.connect(
    host="dxm",
    user="mysql_admin",
    password="tmin",
    database=srcDB
)

 
mydb_target = mysql.connector.connect(
    host="dx",
    user="mxn",
    password="txin",
    database=destDB
)


def getFields(table_name):
    cursor = mydb_target.cursor()
    cursor.execute(f"SHOW columns FROM {table_name}")
    all = cursor.fetchall()
    fields= ''
    dType=[]
    for x in all:
        fields += (',' if fields != '' else '') + (x[0] if x[0] != 'groups' else '`' + x[0] + '`')
        dType.append(x[1])
    cursor.close()
    return (fields,dType)

def getCount(table_name):
    sql_select_Query = f"select count(*) from {table_name}"
    print(sql_select_Query)
    cursor = mydb_src.cursor()
    cursor.execute(sql_select_Query)
    # get all records
    records = cursor.fetchall()
    cursor.close()
    return records[0][0]
        
        
def getData(table_name, fields, n):
    try:
        sql_select_Query = f"select {fields} from {table_name} LIMIT {n}, 100"
        print(sql_select_Query)
        cursor = mydb_src.cursor()
        cursor.execute(sql_select_Query)
        # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)

        print("\nPrinting each row")
        values = []
        for row in records:
            value = ''
            for i in range(len(row)):
                s=row[i]
                if i == 2 and table_name == "xmoptions":
                    s = codecs.decode(row[i])
                    json.loads(s)
                    s = json.dumps(s)                
                colId=1000
                if table_name == "Account":
                    colId=24
                if table_name == "xmoptions":
                    colId = 2
                if (s != None and s != ""):
                    if table_name == "xmoptions":
                        value += (',' if value != '' else '')  +  (s if i == 2 else '"' + str(s) + '"')
                    else:
                        value += (',' if value != '' else '')  +  ('0' if i == colId else "'" + str(s) + "'")
                else:
                    value += ', null'
            values.append(value)
        return values
    except mysql.connector.Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        cursor.close()
        

def getSql(mydb, table_name,fields, data):
    res = []
    for row in data:
        s = f"insert {mydb}.{table_name} ({fields}) values ( {row} )"
        res.append(s)
    return res
        
def runSqls(sqls):
    cursor = mydb_target.cursor()
    for sql in sqls:
        print(sql)
        cursor.execute(sql)
        mydb_target.commit()
    cursor.close()
    
    