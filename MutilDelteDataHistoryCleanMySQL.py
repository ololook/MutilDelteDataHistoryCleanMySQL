#encoding: utf-8
__author__ ='zhangyuanxiang'
import multiprocessing
from multiprocessing import Lock, Process,Value
from ctypes import c_int
import atexit
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import pymysql
from optparse import OptionParser
import time
import os
import pymysql.cursors
counter = Value(c_int) 
counter_lock = Lock()

def increment(num):
    with counter_lock:
         counter.value +=num
         dt=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
         print "delete commit ",counter.value,"Record",dt
    
def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",
                          description="""MySQL Table History Data mrigate""")

    parser.add_option("-H", "--from_dsn",
                      dest="source",
                      default="local2",
                      metavar="host:port:user:passwd:db:table"
                      )

    parser.add_option("-L", "--to_dsn",
                      dest="dest",
                      default="127.0.0.1:3306:0:0:0",
                      metavar="host:port:user;passwd:db:table"
                     )

    parser.add_option("-W", "--where",
                      dest="where",
                      default="1=1",
                      metavar="where",
                      help="source table where default 1=1")

    parser.add_option("-B", "--batch",
                      dest="batch",
                      default=2000,
                      metavar="batch",
                      help="batch size default 2000 row")

    (options, args) = parser.parse_args()

    return options


def from_client(hostport,num=1):

    try:
      host      = hostport.strip().split(':')[0]
      port      = hostport.strip().split(':')[1]
      username  = hostport.strip().split(':')[2]
      password  = hostport.strip().split(':')[3]
      dbname    = hostport.strip().split(':')[4]
      tablename = hostport.strip().split(':')[5]
      
      conn = pymysql.connect(host   =host,
                             port   =int(port),
                             user   =username,
                             passwd =password,
                             db     =dbname,
                             charset='UTF8',
                             cursorclass = pymysql.cursors.SSCursor
                             )
    
    except Exception , e:
                print e
    if num==1:
          return conn.cursor(),dbname,tablename
    elif  num==2:
         return conn
    else:
        return conn.cursor()

def to_client(hostport):

    try:
      host      = hostport.strip().split(':')[0]
      port      = hostport.strip().split(':')[1]
      username  = hostport.strip().split(':')[2]
      password  = hostport.strip().split(':')[3]
      dbname    = hostport.strip().split(':')[4]
      tablename = hostport.strip().split(':')[5]

      conn = pymysql.connect(host   =host,
                             port   =int(port),
                             user   =username,
                             passwd =password,
                             db     =dbname,
                             charset='UTF8'
                             )

    except Exception , e:
           print e

    return conn.cursor(),tablename

def stringsql(db,table1,where):

    FoSql="select * from %s.%s where %s " % (db,table1,where)
    Prosql="""select
                t1.COLUMN_NAME,
                t1.ORDINAL_POSITION
                from information_schema.COLUMNS t1 ,information_schema.STATISTICS t2
                where t1.TABLE_SCHEMA="%s" and t1.table_name=t2.table_name
                and t1.COLUMN_NAME=t2.COLUMN_NAME
                and t2.index_name='PRIMARY'and t1.TABLE_NAME="%s"
               order by ORDINAL_POSITION desc """ %(db,table1)
    
    cntsql="""select
                count(1)
                from information_schema.COLUMNS t1 ,information_schema.STATISTICS t2
                where t1.TABLE_SCHEMA="%s" and t1.table_name=t2.table_name
                and t1.COLUMN_NAME=t2.COLUMN_NAME
                and t2.index_name='PRIMARY'and t1.TABLE_NAME="%s" 
           """ %(db,table1)
    return FoSql,Prosql,cntsql

def insertsql (cursor,table):

        sql_col="""SELECT * from  %s where 1=0 """ % (table)
        row='('
        ncol='(%s'
        insert_sql="insert into %s" %(table)
        cursor.execute(sql_col)
        data = cursor.fetchall()
        for i in range(0, len(cursor.description)):
            if i==0:
               row =row+cursor.description[i][0]
               ncol=ncol
            else:
               row +=','+cursor.description[i][0]
               i=i+1
               ncol +=',%s'
               insert_table=insert_sql+row+' '+')'+' '+'values'+' '+ncol+')'
        return insert_table



def muti_delete(conn,dbname,table,rows,row,lock):
    increment(lock)
    cursor=conn.cursor()
    duparry=[]
    godarry=[]
    dict={}
    del_sql="""delete from %s.%s where 1=1 """ %(dbname,table)
    for r in rows:
        for (name, value) in zip([r[0]],[r[1]]):
               dict[name] = value
        
    for row1 in row:
        for col,val in  dict.items():
            del_sql+=" and %s=%%s " %(col)
            duparry.append(row1[val-1])
        exe_sql= del_sql
        del_sql="""delete from %s.%s where 1=1 """ %(dbname,table)
        godarry.append(duparry)
        duparry=[]
    cursor.executemany(exe_sql,godarry)
    cursor.execute('commit')
    conn.close()

def export_data():
        row=[]
        jobs = []
        offset=800
        options = get_cli_options()
        batch=int(options.batch)
        count=0
        Focursor,dbname,table1=from_client(options.source)
        cntcursor=from_client(options.source,0)
        ToFocursor,table2=to_client(options.dest)
        FoSql,Prosql,cntsql=stringsql(dbname,table1,options.where)  
        Tosql=insertsql(ToFocursor,table2)
        cntcursor.execute(cntsql)
        pri  =cntcursor.fetchone()
        if pri[0]==0:
           print 
           print "%s is not primary key!!! exit() " %(table1)
           print 
           exit()
        cntcursor.close()
        Focursor.execute(Prosql)
        rows = Focursor.fetchall()
        #lock = multiprocessing.Lock()   
        Focursor.execute(FoSql)
        num_fields = len(Focursor.description)
        print batch
        row = Focursor.fetchmany(batch)
        while row:
              lg=len(row)
              ToFocursor.executemany(Tosql,row)
              ToFocursor.execute('commit')
              for i in range(0,lg,offset):
                  conn=from_client(options.source,2)
                  p = multiprocessing.Process(target=muti_delete,args=(conn,dbname,table1,rows,row[i:i+offset],len(row[i:i+offset])))
                  p.start()
                  jobs.append(p)
              row=[]
              row = Focursor.fetchmany(batch)
              for j in jobs:
                  j.join()
                  j.terminate()
                
def main():
    export_data()

if __name__ == '__main__':
    main()
