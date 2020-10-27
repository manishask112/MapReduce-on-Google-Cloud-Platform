from flask import Flask, request
import requests, re, time
import multiprocessing
import logging
import googleapiclient.discovery
from oauth2client.client import GoogleCredentials

app = Flask(__name__)
Num_Of_Map_Reduce_Workers = 4
reduce_num = 0
key_value_ip_address = ''

credentials = GoogleCredentials.get_application_default()
compute = googleapiclient.discovery.build('compute', 'v1', credentials = credentials)

# log = logging.getLogger('werkzeug')
# log.setLevel(logging.ERROR)

def wait_for_operation(operation,i):
    global compute
    project = 'manisha-suresh'
    zone = 'us-central1-a'
    # Code taken from https://github.com/GoogleCloudPlatform/python-docs-samples/blob/9c782aa00afa19812974c8a391ba32fe8ffba865/compute/api/create_instance.py#L128
    print('Waiting for MapReduce ' + str(i + 1) + ' to start/stop...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return True
        time.sleep(1)
    # Code from 'GoogleCloudPlatform/python-docs-samples' ends here
    pass

# ********************************************************************************
# **************************CODE FOR MASTER BEGINS********************************
@app.route('/run_mapred/<input_data>/<int:map_num>/<int:numberOfReducers>/<map_fn>/<reduce_fn>', methods = ['GET'])
def run_mapred(input_data, map_num, numberOfReducers, map_fn, reduce_fn):
    logging.basicConfig(filename='MasterLogs.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    global Num_Of_Map_Reduce_Workers
    global key_value_ip_address
    global reduce_num
    reduce_num = numberOfReducers
    #*******Splitting data into chunks*******
    data = []
    j = 1
    data_len = 0
    input_data = input_data.split(':')
    for input_file in input_data:
        fp = open(input_file, encoding = "utf8")
        file_data = fp.read()
        file_data = file_data.split()
        match = re.compile('[\W_]+')
        for i, word in enumerate(file_data):
            word = word.lower()
            file_data[i] = (match.sub('', word), input_file)
            data_len += 1
            data.append(file_data[i])
        # j += 1

    len_of_chunks = data_len // map_num

    # GET IP ADDRESS OF KVS
    project = 'manisha-suresh'
    zone = 'us-central1-a'
    ip_address_response = compute.instances().get(project = project, zone = zone, instance = 'instance-key-value-store').execute()
    key_value_ip_address = ip_address_response['networkInterfaces'][0]['accessConfigs'][0]['natIP']
    # *******Persist input data (chunks) into Key Value store
    for i in range(1,map_num + 1):
        input_data_local_storage = data[0:len_of_chunks]
        # url = 'https://compute.googleapis.com/compute/v1/projects/' + 'manisha-suresh' + '/zones/' + 'us-central1-a' + '/instances/' + 'instance-key-value-store'
        url = 'http://' + key_value_ip_address + ':' + str(5000) + '/put/' + 'intermediate_file_' + str(i)
        response = requests.post(url, json = {'input_data' : input_data_local_storage})
        if response.status_code >= 400 or response.status_code >= 500:
            logging.error(response.reason)
        else:
            logging.info('Persisted ' + list(response.json().keys())[0] + ' into KV Store')
        data = data[len_of_chunks:]


    print("\n\nMap tasks are running...")

    # ********LETS START THE MAPPER CONCURRENTLY*******

    image_response = compute.images().get(project=project, image='image-map-reduce').execute()
    source_disk_image = image_response['selfLink']
    machine_type = 'projects/manisha-suresh/zones/' + zone + '/machineTypes/e2-medium'
    startup_script = 'python3 MapReduce.py'
    mapReduceConfig = {
        'name': '',
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }]
        }
    }
    VMInstances = []
    # Num_Of_Map_Reduce_Workers = 4
    for i in range(1,Num_Of_Map_Reduce_Workers + 1):
        mapReduceConfig['name'] = 'map-reduce' + str(i)
        VMInstances.append(compute.instances().insert(project=project, zone=zone, body=mapReduceConfig).execute())

    for i, VMInstance in enumerate(VMInstances):
        wait_for_operation(VMInstance['name'], i) #A function that waits for the instance to be started. 

    filesPerMapper = map_num // Num_Of_Map_Reduce_Workers
    j = 1
    mapProcesses = []
    mapReduceInstanceIPAddresses = []
    for i in range(1,Num_Of_Map_Reduce_Workers + 1):
        ip_address_response = compute.instances().get(project = project, zone = zone, instance = 'map-reduce' + str(i)).execute()
        mapReduceInstanceIPAddresses.append(ip_address_response['networkInterfaces'][0]['accessConfigs'][0]['natIP']) # This holds
        if i == Num_Of_Map_Reduce_Workers + 1:
            filesPerMapper += (map_num % Num_Of_Map_Reduce_Workers)
        url = 'http://' + mapReduceInstanceIPAddresses[i - 1] + ':' + str(5000) + '/mapWorker/' + map_fn + '/intermediate_file_/' + str(j) + '/' + str(j+filesPerMapper-1)
        mapProcesses.append(multiprocessing.Process(target=requests.post, args=(url, )))
        j += filesPerMapper

    for p in mapProcesses:
        # starting process
        p.start()

    for p in mapProcesses:
        # wait until processes finish 
        p.join()
    logging.info('Mapper Task : ' + list(response.json().keys())[0] + ' successfully completed')

    # for response in response_list:
    #     if response.status_code >= 400 or response.status_code >= 500:
    #         logging.error(response.reason)
    #     else:
    #         logging.info('Mapper Task : ' + list(response.json().keys())[0] + ' successfully completed')

    # Partitioning data for reduce tasks
    url = 'http://' + mapReduceInstanceIPAddresses[0] + ':' + str(5000) + '/reduceWorker/' + reduce_fn + '/' + 'intermediate_file_/' + str(1) + '/' + str(map_num) + '/' + int(reduce_num)  + '/' + int(map_num)
    response = requests.get(url)
    if response.status_code >= 400 or response.status_code >= 500:
        logging.error(response.reason)
    else:
        logging.info('Partition Task : ' + list(response.json().keys())[0] + ' successfully completed')

    # *********LETS START THE REDUCERS CONCURRENTLY*******
    print("\n\nReduce tasks are running...")
    filesPerMapper = reduce_num // Num_Of_Map_Reduce_Workers
    j = 1
    reduceProcesses = []
    for i in range(1,Num_Of_Map_Reduce_Workers + 1):
        if i == Num_Of_Map_Reduce_Workers + 1:
            filesPerMapper += (reduce_num % Num_Of_Map_Reduce_Workers)
        url = 'http://' + mapReduceInstanceIPAddresses[i - 1] + ':' + str(5000) + '/reduceWorker/' + reduce_fn + '/' + 'intermediate_file_/' + str(j) + '/' + str(j+filesPerMapper-1) + '/' + int(reduce_num)  + '/' + int(map_num)
        mapProcesses.append(multiprocessing.Process(target=requests.post, args=(url, )))
        j += filesPerMapper

    for p in mapProcesses:
        # starting process
        p.start()

    for p in mapProcesses:
        # wait until processes finish 
        p.join()

    # DATA CAN NOW BE FOUND IN OUTPUT FILE IN READABLE FORMAT
    output_text = ''
    url = 'http://' + key_value_ip_address + ':' + 5000 + '/get/' + 'final.txt'
    response = requests.get(url)
    output_text = list(response.json().values())[0]
    if response.status_code >= 400 or response.status_code >= 500:
            logging.error(response.reason)
    else:
        logging.info('Fetched final data for Master from KV Store')
    return {'OUTPUT_TEXT' : output_text}
# ****************************CODE FOR MASTER ENDS********************************
# ********************************************************************************

@app.route('/delete_cluster', methods = ['DELETE'])
def delete_cluster():
    global Num_Of_Map_Reduce_Workers
    global key_value_ip_address
    global reduce_num
    global compute

    # Stop MapReduce Instances 
    VMInstances = []
    project = 'manisha-suresh'
    zone = 'us-central1-a'
    for i in range(1, Num_Of_Map_Reduce_Workers + 1):
        name = 'map-reduce' + str(i)
        VMInstances.append(compute.instances().delete(project=project, zone=zone, instance=name).execute())
    for i, VMInstance in enumerate(VMInstances):
        wait_for_operation(VMInstance['name'], i)

    # Deleting intermediate files
    for i in range(1,reduce_num + 1):
        url = 'http://' + key_value_ip_address + ':' + str(5000) + '/delete_file/' + 'intermediate_file_' + str(i)
        response = requests.delete(url)

        if response.status_code >= 400 or response.status_code >= 500:
            logging.error(response.reason)
        else:
            logging.info('Intermediate file : ' + str(i) + ' deleted from KV Store')

    return {'DELETED_intermediate_files' : 1}


# def init_cluster(ip_address, port):
app.run(host = '0.0.0.0', port = 5000, debug=False)