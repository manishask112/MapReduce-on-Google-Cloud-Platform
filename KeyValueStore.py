from flask import Flask, request
import pickle
from threading import Lock
import os

app = Flask(__name__)
lock = Lock()

# Function to check whether file storage exists
# (for the very first persistance)
def file_exists(file_name):
    return os.path.exists(file_name)

# Function to extract all key-value pairs from pickled data in file
def get_all_data(file_name):
    f = open(file_name, 'rb')
    message = pickle.load(f)
    f.close()
    return message

# Function to write pickled data to file
def write_to_file(file_name,message_dict):
    f = open(file_name, 'wb')
    pickle.dump(message_dict,f)
    f.close()

def write_to_text_file(file_name,data):
    # print(type(data))
    f = open(file_name, '+a')
    for d in data:
        f.write(d[0] + ' - ' + str(d[1]) + '\n')
    f.close()

@app.route('/get/<file_name>',methods = ['GET'])
def get(file_name):
    lock.acquire()
    data = None
    if file_exists(file_name):
        data = get_all_data(file_name)
    lock.release()
    return {'data' : data}

@app.route('/put/<file_name>',methods = ['POST'])
def put(file_name):
    lock.acquire()
    data_dict = request.get_json()
    key = list(data_dict.keys())[0]
    data = list(data_dict.values())[0]
    if key == 'final_docs':
        write_to_text_file(file_name,data)
    else:
        write_to_file(file_name,data)    
    lock.release()
    return {key + ' '+ file_name.split('_')[-1] : 1}

@app.route('/delete_file/<file_name>',methods = ['DELETE'])
def delete_file(file_name):
    lock.acquire()
    if file_exists(file_name):
        os.remove(file_name)
    lock.release()
    return {'DELETED' : 1}
   
# def startKeyValueStore(ip_address, port):
app.run(host = '127.0.0.1', port = 5000, debug=False)