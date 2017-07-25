from celery import Celery
from subprocess import call
import json
import pyhs2

app = Celery('call_distcp', backend='amqp://', broker='amqp://')

@app.task
def distcp(src_url, dis_url):
    return call(
        ['hadoop', 'distcp', src_url, dis_url])
@app.task
def wanda_import_table(src_url, dst_url, table_name, hive_table_meta):

    # print async_tasks.distcp(src_url, dst_url)
    
    try:
        with pyhs2.connect(host='10.141.220.200',port=10000,database='default',authMechanism='PLAIN',user='hive',password='123456') as con: 
            with con.cursor() as cur:
                query = "create external table hive_" + table_name + "(" + hive_table_meta + ")" + "row format serde 'parquet.hive.serde.ParquetHiveSerDe' stored as inputformat 'parquet.hive.DeprecatedParquetInputFormat' outputformat 'parquet.hive.DeprecatedParquetOutputFormat' location '" + dst_url +"'"
                print(query)
                cur.execute(query)
                return 0
    except Exception,e:
        print 'Exception found in wanda_import_table: ',e
        return 1

@app.task
def distcp_and_import(
        src_data_block_url,
        table_name,
        hive_table_meta,
        sandbox_ip,
        file_path):
    dis_data_block_url = 'hdfs://' + sandbox_ip + ':9000' + file_path

    status_distcp = []
    status_distcp.append(
            distcp(src_data_block_url, dis_data_block_url))


    status_import = []
    try:
        with pyhs2.connect(host=sandbox_ip, port=10000, authMechanism="PLAIN", user='hadoop', password='hadoop', database='default') as conn:
            with conn.cursor() as cur:
                # Show databases
                # print cur.getDatabases()
                query = "create external table " + table_name + \
                    "(" + hive_table_meta + ")" + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION " + "'" + file_path + "'"
                print(query)
                print(cur.execute(query))
                result = 0
    except BaseException:
        result = 1
    status_import.append(result)

    print [status_distcp, status_import]
