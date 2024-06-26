import utils

 
tables = ['Account','Address','AgentRole','ApiStatus']



tables= ['Account']
tables= ['User']
srcDB = 'mydb'
destDB = 'mydb'


(fields, dType) = utils.getFields(tables[0])
# print(fields, dType)
count = utils.getCount(tables[0])
for i in range(count):
    try:
        data = utils.getData(tables[0], fields, i)
        sqls = utils.getSql(destDB, tables[0], fields, data)
        print(sqls)
        utils.runSqls(sqls)
    except Exception as e:
        print(e)
        print(i)     


     