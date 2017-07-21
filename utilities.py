import subprocess
from subprocess import call
import json
import pyhs2
import async_tasks

def call_distcp(src_url, dis_url):
    async_tasks.distcp.delay(src_url, dis_url)

def distcp_and_import(src_url, table_name, data_meta, sandbox_ip, file_path):
    async_tasks.distcp_and_import.delay(src_url, table_name, data_meta, sandbox_ip, file_path)

def import_external_table(host, port, table_name, import_path):
    try:
        with pyhs2.connect(host=host, port=port, authMechanism="PLAIN", user='hadoo', password='hadoop', database='default') as conn:
            with conn.cursor() as cur:
                print cur.getDatabases()
                query = "import external table " + table_name + " from \"" + import_path + "\""
                print(query)
                # cur.execute(query)
                return 0
    except BaseException:
        return 1


def call_get_hdfs_address(table_name, sandbox_ip):
    sandbox_ip = '10.132.141.120'
    try:
        with pyhs2.connect(host=sandbox_ip, port=10000, authMechanism="PLAIN", user='hive', password='hive', database='bigdata') as conn:
            with conn.cursor() as cur:
                # Show databases
                # print cur.getDatabases()
                query = "desc formatted " + table_name
                print(query)
                cur.execute(query)
                for i in cur.fetch():
                    # print(i)
                    # print(i[0])
                    if(i[0] == 'Location:           '):
                        return i[1].split("9000")[1]
    except BaseException:
        return None


def call_get_meta_data(table_name):
    try:
        with pyhs2.connect(host=sandbox_ip, port=10000, authMechanism="PLAIN", user='hadoop', password='hadoop', database='default') as conn:
            with conn.cursor() as cur:
                # Show databases
                # print cur.getDatabases()
                cur.execute(query)
                meta_data = []
                for i in cur.fetch():
                    if(i[0] == '' and flag == 0):
                        flag = 1
                    elif(i[0] == '' and flag == 1):
                        flag = 2
                    elif(flag == 1):
                        meta_data.append((i[0].strip(), i[1].strip()))
                return meta_data
    except BaseException:
        return None

def transform_hive_meta(hive_table_meta):
    temp1 = hive_table_meta.split(",")
    temp_list = []
    for i in temp1:
        temp2 = i.split("|")
        temp2.reverse()
        temp_list.append(" ".join(temp2))
    temp3 = ",".join(temp_list)
    return temp3[0:-1]
