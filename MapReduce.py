from flask import Flask, request
import requests, re, os
import logging
import googleapiclient.discovery
from oauth2client.client import GoogleCredentials

app = Flask(__name__)
# key_value_ip_address = ''

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="manisha-suresh-400bdc514e97.json"

credentials = GoogleCredentials.get_application_default()
compute = googleapiclient.discovery.build('compute', 'v1', credentials = credentials)
project = 'manisha-suresh'
zone = 'us-central1-a'
ip_address_response = compute.instances().get(project = project, zone = zone, instance = 'instance-key-value-store').execute()
key_value_ip_address = ip_address_response['networkInterfaces'][0]['accessConfigs'][0]['natIP']


logging.basicConfig(filename='MapReduceLogs.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# log = logging.getLogger('werkzeug')
# log.setLevel(logging.ERROR)   

def map_id(key,val):
    word, document_num = key
    return [word,[document_num, val]]
def map_wc(key,val):
    word, document_num = key
    return [word,[document_num, val]]

def reduce_id(word_doc_count, value_list):
    w,[d,c] = word_doc_count
    if value_list == []:
        return [{d : c}]
    if d in value_list[0]:
        value_list[0][d] = int(value_list[0][d]) + int(c)
    else:
        value_list[0][d] = c

    return value_list
def reduce_wc(word_doc_count, value_list):
    w,[d,c] = word_doc_count
    if value_list == []:
        value_list =  [c]
    else:
        value_list[0] = int(value_list[0]) + int(c)

    return value_list

MAPPER_FUNCTIONS = {'map_id':map_id, 'map_wc':map_wc}
REDUCER_FUNCTIONS = {'reduce_id':reduce_id, 'reduce_wc':reduce_wc}

# ********************************************************************************
# **************************CODE FOR MAPPER BEGINS********************************
@app.route('/mapWorker/<map_fn>/<input_file_name>/<int:first_file_number>/<int:last_file_number>',methods = ['POST'])
def map_worker(map_fn, input_file_name, first_file_number, last_file_number):
    global key_value_ip_address
    global compute

    # Get IP ADDRESS OF KVS
    for i in range(first_file_number, last_file_number + 1):
        url = 'http://' + key_value_ip_address + ':' + str(5000) + '/get/' + input_file_name + str(i)
        response = requests.get(url)
        input_file = list(response.json().values())[0]
        if response.status_code >= 400 or response.status_code >= 500:
                logging.error(response.reason)
        else:
            logging.info('Fetched data for MAP WORKER ' + str(i) + ' from KV Store')

        mapped_values = []
        if input_file_name:
            for v in input_file:
                mapped_values.append(MAPPER_FUNCTIONS[map_fn](v,1))
        url = 'http://' + key_value_ip_address + ':' + str(5000) + '/put/' + input_file_name + str(i)
        response = requests.post(url, json = {'intermediate_data' : mapped_values})
        if response.status_code >= 400 or response.status_code >= 500:
                logging.error(response.reason)
        else:
            logging.info('Persisted ' + list(response.json().keys())[0] + ' into KV Store')


    return {'MAPPER_' + str(input_file_name.split('_')[-1]) : 1}
# ****************************CODE FOR MAPPER ENDS********************************
# ********************************************************************************

# ********************************************************************************
# *************************CODE FOR REDUCER BEGINS********************************
@app.route('/reduceWorker/<reduce_fn>/<intermediate_file_name>/<int:first_file_number>/<int:last_file_number>/<int:reduce_num>/<int:map_num>',methods = ['GET','POST'])
def reduce_worker(reduce_fn, intermediate_file_name, first_file_number, last_file_number, reduce_num, map_num):
    global key_value_ip_address
    global compute

    # project = 'manisha-suresh'
    # zone = 'us-central1-a'
    # ip_address_response = compute.instances().get(project, zone, 'instance-key-value-store')
    # key_value_ip_address = ip_address_response['accessConfigs'][0]['natIP']

    if request.method == 'GET':

        intermediate_data = []
        for i in range(1,map_num + 1):
            url = 'http://' + key_value_ip_address + ':' + str(5000) + '/get/' + intermediate_file_name + str(i)
            response = requests.get(url)
            input_file = list(response.json().values())[0]
            intermediate_data.extend(input_file)
        count = 0
        partitionedData = {}
        for i in range(1,reduce_num + 1):
            partitionedData[i] = []
        for word,[doc,c] in intermediate_data:
            hash_word = int(hash(word)) % reduce_num + 1
            partitionedData[hash_word].append([word,[doc,c]])
            partitionedData[hash_word] = sorted(partitionedData[hash_word], key = lambda x : x[0])
        #for i in range(1,reduce_num + 1):
            #print("Partition " + str(i) + ' : ' + str(len(partitionedData[i])))
        for key, data in partitionedData.items():
            # input_data_local_storage = data[0:len_of_chunks]
            url = 'http://' + key_value_ip_address + ':' + str(5000) + '/put/' + intermediate_file_name + str(key)
            response = requests.post(url, json = {'partitioned_data' : data})
            if response.status_code >= 400 or response.status_code >= 500:
                logging.error(response.reason)
            else:
                logging.info('Persisted ' + list(response.json().keys())[0] + ' into KV Store')

        url = 'http://' + key_value_ip_address + ':' + str(5000) + '/delete_file/' + 'final.txt'
        response = requests.delete(url)

        return {'REDUCE_WORKER_PARTITION':1}

    if request.method == 'POST':

        logging.info("THIS REDUCER WILL WORK ON FILES " + str(first_file_number) +' to '+ str(last_file_number))
        # GET PARTITION FOR THIS REDUCER TASK
        for i in range(first_file_number, last_file_number + 1):
            url = 'http://' + key_value_ip_address + ':' + str(5000) + '/get/' + intermediate_file_name + str(i)
            response = requests.get(url)
            intermediate_file = list(response.json().values())[0]
            if response.status_code >= 400 or response.status_code >= 500:
                    logging.error(response.reason)
            else:
                logging.info('Fetched data for REDUCE WORKER ' + str(i) + ' from KV Store')

            previous_word = None
            final_list = []
            for [word,[doc,c]] in intermediate_file:
                if not previous_word:
                    value_list = REDUCER_FUNCTIONS[reduce_fn]([word,[doc,c]], [])
                    previous_word = word
                elif word == previous_word:
                    value_list = REDUCER_FUNCTIONS[reduce_fn]([word,[doc,c]], value_list)
                else:
                    final_list.append([previous_word,value_list])
                    logging.info(final_list)
                    value_list = REDUCER_FUNCTIONS[reduce_fn]([word,[doc,c]], [])
                    previous_word = word
                # print(word, value_list)
            if value_list != []:
                final_list.append([word,value_list[0]])

            url = 'http://' + key_value_ip_address + ':' + str(5000) + '/put/' + 'final.txt'
            response = requests.post(url, json = {'final_docs' : final_list})
            if response.status_code >= 400 or response.status_code >= 500:
                    logging.error(response.reason)
            else:
                logging.info('Persisted ' + list(response.json().keys())[0] + ' into final Output File')
    return {'REDUCER_' + str(intermediate_file_name.split('_')[-1]) : 1}
# ***************************CODE FOR REDUCER ENDS********************************
# ********************************************************************************

app.run(host = '0.0.0.0', port = 5000, debug=False)
                                                            