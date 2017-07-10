import subprocess
from subprocess import call
import json
def import_external_table(host,port,table_name,import_path):
  try:
    with pyhs2.connect(host=host,port=port,authMechanism="PLAIN",user='hadoo',password='hadoop',database='default') as conn:
      with conn.cursor() as cur:
        print cur.getDatabases()
        query="import external table "+table_name+" from \""+import_path+"\""
        print(query)
        #cur.execute(query)
        return 0
  except:
    return 1

def call_get_hdfs_adress(table_name,sandbox_ip):
   try:
      with pyhs2.connect(host=sandbox_ip,port=10000,authMechanism="PLAIN",user='hadoop',password='hadoop',database='default') as conn:
         with conn.cursor() as cur:
         #Show databases
         #print cur.getDatabases()
            query="desc formatted "+table_name
            print(query)
            cur.execute(query)
            for i in cur.fetch():
               #print(i)
               #print(i[0])
               if(i[0]=='Location:           '):
                  return i[1].split("9000")[1]
   except:
      return None

def call_get_meta_data(table_name):
   try:
      with pyhs2.connect(host=sandbox_ip,port=10000,authMechanism="PLAIN",user='hadoop',password='hadoop',database='default') as conn:
         with conn.cursor() as cur:
         #Show databases
         #print cur.getDatabases()
           cur.execute(query)
           meta_data=[]
           for i in cur.fetch():
               if(i[0]=='' and flag==0):
                  flag=1
               elif(i[0]=='' and flag==1):
                  flag=2
               elif(flag==1):
                  meta_data.append((i[0].strip(),i[1].strip()))
           return meta_data
   except:
      return None

def call_distcp(source_data_block_url,dis_data_block_url):
    ret_val=call(["hadoop", "distcp",source_data_block_url,dis_data_block_url])
    return ret_val

def call_import_external(table_name,hive_table_meta,sandbox_ip,dis_data_block_url):
  try:
    with pyhs2.connect(host=sandbox_ip,port=10000,authMechanism="PLAIN",user='hadoop',password='hadoop',database='default') as conn:
      with conn.cursor() as cur:
        #Show databases
        #print cur.getDatabases()
        query="create external table "+table_name+"("+hive_table_meta+")"+" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION "+"'"+dis_data_block_url+"'"
        print(query)
        print(cur.execute(query))
        return 0
  except:
    return 1

def transform_hive_meta(hive_table_meta):
    temp1=hive_table_meta.split(",")
    temp_list=[]
    for i in temp1:
        temp2=i.split("|")
        temp2.reverse()
        temp_list.append(" ".join(temp2))
    temp3=",".join(temp_list)
    return temp3[0:-1]
