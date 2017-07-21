#!flask/bin/python
from flask import Flask, jsonify, request
#from flask.ext.cors import CORS
import json
app = Flask(__name__)
# CORS(app)
from utilities import *


@app.route('/dataquality/api/sendDataToPortal', methods=['GET'])
def sendDataToPortal():
    hive_table_list = json.loads(request.args.get('hive_table_list'))

    for index, temp in enumerate(hive_table_list):
        table_name = temp['table_name']
        sandbox_ip = temp['sandbox_ip'][1:-1]
        source_data_block_url = call_get_hdfs_address(table_name, sandbox_ip)
        dis_data_block_url = temp['hdfs_dis_data_block_url']
        hive_table_meta = call_get_meta_data(table_name)

        print(dis_data_block_url)
        print(table_name)
        print(source_data_block_url)
        call_distcp(source_data_block_url, dis_data_block_url)
        call_import_external_wanda(
                table_name,
                hive_table_meta,
                wanda_ip,
                dis_data_block_url)

    return jsonify({'status': "okay"}), 200
    


@app.route('/dataquality/api/loadDateFromPortal', methods=['POST'])
def loadDateFromPortal():
    return jsonify({'status': "okay"}), 200


@app.route('/dataquality/api/loadDateFromPortal', methods=['GET'])
def loadDateFromProtal():
    #response.headers['Access-Control-Allow-Origin'] = '*'
    #response.headers['Access-Control-Allow-Methods'] = 'POST'
    #response.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'
    hive_table_list = json.loads(request.args.get('hive_table_list'))
    sandbox_ip = request.args.get('sandbox_ip')[1:-1]
    job_id = request.args.get('job_id')
    print(sandbox_ip)
    print(job_id)
    # print(hive_table_list)
    # print(len(hive_table_list))
    len_list = len(hive_table_list)
    status1 = []
    status2 = []
    for index, temp in enumerate(hive_table_list):
        # print(str(index)+":"+temp['table_name'])
        # print(str(index)+":"+temp['hdfs_source_data_block_url'])
        # print(str(index)+":"+temp['hive_table_meta'])
        hive_table_meta = transform_hive_meta(temp['hive_table_meta'])
        source_data_block_url = temp['hdfs_source_data_block_url']
        table_name = temp['table_name']
        file_path = "/" + str(job_id) + "/" + table_name
        dis_data_block_url = "hdfs://" + sandbox_ip + ":9000" + file_path
        print(source_data_block_url)
        print(table_name)
        print(dis_data_block_url)
        # print(hive_table_meta)
        distcp_and_import(
                source_data_block_url,
                table_name,
                hive_table_meta,
                sandbox_ip,
                file_path)
    return jsonify({'status': "okay"}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
