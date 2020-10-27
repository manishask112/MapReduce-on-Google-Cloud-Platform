import requests
from importlib import import_module
from multiprocessing import Process
# import googleapiclient.discovery


if __name__ == '__main__':

    config = 'config.py'

    info = import_module(config.split('.')[0])
    input_data, map_num, numberOfReducers, map_fn, reduce_fn, output_location, ip_address, port = info.input_data, info.map_num, info.reduce_num, info.map_fn, info.reduce_fn, info.output_location, info.ip_address, info.port
    colon_seperated_input_data = ''
    for file in input_data:
        colon_seperated_input_data = colon_seperated_input_data + file + ':'

    url = 'http://' + ip_address + ':' + str(port) + '/run_mapred/' + colon_seperated_input_data[:-1] + '/' + str(map_num) + '/' + str(numberOfReducers) + '/' + map_fn +'/' + reduce_fn
    r = requests.get(url)
    output_text = list(r.json().values())[0]
    f = open(output_location, 'w')
    for output in output_text:
        # output = output.lstrip('\\n')
        f.write(output)
    f.close()
    # Delete Intermediate files and stop VMs
    url = 'http://' + ip_address + ':' + str(port) + '/delete_cluster'
    # r = requests.delete(url)


    print('\n\n***********************************************************')
    # print("***********Check \'" + info.output_location + '\' for the output*********')
    print('***********************************************************\n\n')


