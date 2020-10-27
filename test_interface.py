import requests
from importlib import import_module
from multiprocessing import Process

if __name__ == '__main__':
    configs = ['ip_port_config_for_test.py','test_config_1.py', 'test_config_2.py', 'test_config_3.py', 'test_config_4.py']
    info = import_module(configs[0].split('.')[0])
    ip_address, port = info.ip_address, info.port

    # Run the MASTER
    print("Running 4 test cases. This might take a while...\n")
    # len(configs)
    for i in range(1,len(configs)):
        # config = configs[i]
        info = import_module(configs[i].split('.')[0])
        input_data, map_num, numberOfReducers, map_fn, reduce_fn, output_location = info.input_data, info.map_num, info.reduce_num, info.map_fn, info.reduce_fn, info.output_location
        colon_seperated_input_data = ''
        for file in input_data:
            colon_seperated_input_data = colon_seperated_input_data + file + ':'

        url = 'http://' + ip_address + ':' + str(port) + '/run_mapred/' + colon_seperated_input_data[:-1] + '/' + str(map_num) + '/' + str(numberOfReducers) + '/' + map_fn +'/' + reduce_fn
        r = requests.get(url)
        output_text = list(r.json().values())[0]
        f = open(output_location, 'w')
        for output in output_text:
            f.write(output)
        f.close()
        # Delete Intermediate files and stop VMs
        url = 'http://' + ip_address + ':' + str(port) + '/delete_cluster'
        r = requests.delete(url)


        print('\nTEST CASE ' + str(i) + ' completed')

    print('\n\n***********************************************************')
    # print("***********Check \'" + info.output_location + '\' for the output*********')
    print('***********************************************************\n\n')