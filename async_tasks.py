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
