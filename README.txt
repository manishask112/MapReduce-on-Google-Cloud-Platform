1.  Execute the below command for test cases. There are four test cases. 
    Two for each application (word count (wc) and inverse document (id)) 
    and one output file for each test case will be generated and named 'test_case_<1/2/3/4>_final_<wc/id>.txt'

        python3 test_interface.py

    Note : Test case 3 has three documents as input and hence may take a while (although, well less than a minute)

2.  Logs can be found in MapReduceLogs.log

3.  if you want to try with your own test case, edit 'config.py' file and the execute

        python3 interface.py

    Make sure the input text file is in this same directory with all other files.