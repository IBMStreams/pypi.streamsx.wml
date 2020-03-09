from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.context import ConfigParams
import streamsx.topology.context
import streamsx.rest as sr
import unittest
import os
import json
import time
import streamsx.wml as wml
import streamsx.wml.utils as wml_utils
from streamsx.wml.bundleresthandler.wmlbundleresthandler import WmlBundleRestHandler
from streamsx.wml.bundleresthandler.bundleresthandler import BundleRestHandler

import threading
import numpy

from watson_machine_learning_client import WatsonMachineLearningAPIClient






#tracer.setLevel(logging.DEBUG)
#tracer.addHandler(logging.StreamHandler(sys.stdout))


###################################################################################
# Read credentials from test environment
###################################################################################
def cloud_creds_env_var():
    result = {}
    try:
        result = os.environ['MACHINE_LEARNING_SERVICE_CREDENTIALS']
    except KeyError: 
        result = None
    return result

def deployment_env_var():
    result = None
    try:
        result = os.environ['WML_DEPLOYMENT_GUID']
    except KeyError: 
        result = None
    return result
    
def space_env_var():
    result = None
    try:
        result = os.environ['WML_SPACE_GUID']
    except KeyError: 
        result = None
    return result



###################################################################################
# Class used as output object in bundleresthandler tests 
###################################################################################
class output_class():
    def __init__(self, output_object):
        self._output_object = output_object
    def __call__(self, output):
        #with self._output_lock:
        print ("################ stream output ######################")
        print ( str(output))
        print ("#####################################################")


###################################################################################
# Class with callable as test source
###################################################################################
class TestSource:
    def __init__(self, ):
        pass
    def __call__(self):
            # run this indefinitely so that there will always be data for the view
        counter = 0
        while True:
            counter += 1
            #see if streaming source is loosing tuples 
            if counter == 10001:
                time.sleep(30)
            record = {"petal_length":1.4,
                       "petal_width":0.2,
                       "sepal_length":5.1,
                       "sepal_width":3.5,
                       "number" : counter}
            # generate errors
            if counter % 20 == 0:
                record.pop("petal_length",None)
                record.pop("sepal_length",None)

            #yield everytime same values, doesn't matter for test
            yield record




###################################################################################
# UNIT TESTS
###################################################################################
class Test(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print (str(self))
        
    def _build_only(self, name, topo):
        test_config={}
        test_config[ConfigParams.SSL_VERIFY] = False  

        result = streamsx.topology.context.submit("TOOLKIT", topo.graph,test_config ) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph,test_config)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

    def _get_credentials(self):
        if cloud_creds_env_var() == True:
            creds_file = os.environ['MACHINE_LEARNING_SERVICE_CREDENTIALS']
            with open(creds_file) as data_file:
                credentials = json.load(data_file)
        else:
            credentials = json.loads('{"username" : "user", "password" : "xxx", "url" : "xxx", "instance_id" : "xxx"}')
        return credentials


    #########################################################################
    # Test the complete ooperator in a topology application
    #
    # actually there is no stop criteria, neither in application
    # nor externally
    #########################################################################
    def test_score_bundle(self):
        print ('\n---------'+str(self))

        field_mapping =[{'model_field':'Sepal.Length',
                            'is_mandatory':True,
                            'tuple_field':'sepal_length'},
                           {'model_field':'Sepal.Width',
                            'is_mandatory':True,
                            'tuple_field':'sepal_width'},
                           {'model_field':'Petal.Length',
                            'is_mandatory':True,
                            'tuple_field':'petal_length'},
                           {'model_field':'Petal.Width',
                            'is_mandatory':True,
                            'tuple_field':'petal_width'}]

        deployment_guid = deployment_env_var() #'a0a04976-9a81-4748-bed0-079890f7c96c'
        space_guid = space_env_var() #'062c92c1-765e-43b9-801a-629215a0e866'
        
        name = 'test_score_bundle'
        topo = Topology(name)
        source_stream = topo.source(TestSource())
        # stream of dicts is consumed by wml_online_scoring
        scorings,invalids = wml.wml_online_scoring(source_stream,
                                     deployment_guid,
                                     field_mapping, 
                                     json.loads(cloud_creds_env_var()), #wml_credentials,
                                     space_guid,
                                     expected_load = 1000,
                                     queue_size = 2000, 
                                     threads_per_node = 1)

        #print_stream = scorings.map(lambda t: print(str(t)))

        scorings.publish(topic="ScoredRecords")
        invalids.publish(topic="InvalidRecords")

        #res.print()
        if (("TestDistributed" in str(self)) or ("TestStreamingAnalytics" in str(self))):
            self._launch(topo)
        else:
            # build only
            self._build_only(name, topo)
            
            
    #########################################################################
    # Test the bundleresthandler base class
    # - copy N (configured) tuples from global queue
    # - check the global queue size after two copies of 
    #   different handlers 
    # 
    #########################################################################
    def test_BundleRestHandler_copy(self):
        print("############# test_WmlBundleRestHandler_preprocess() ###############")

        #create instance by copying from a source_list
        source_list = [{"a":i} for i in range(10)]

        #set class variables
        BundleRestHandler.max_copy_size = 2
        #lock = threading.Lock()
        lock = threading.Condition()
        BundleRestHandler.input_list_lock = lock
        BundleRestHandler.source_data_list = source_list
        BundleRestHandler.field_mapping = []
        BundleRestHandler.output_function = (lambda x: print(str( x)))
        
        test_store1 = BundleRestHandler(1)
        test_store1.copy_from_source()
        #print ("source_list: ", source_list)
        print("    check source list length after copy: handler 1")
        assert len(source_list) == 8
        BundleRestHandler.max_copy_size = 4
        test_store2 = BundleRestHandler(2)
        test_store2.copy_from_source()
        #print ("source_list: ", source_list)
        #print ("local_list 1: ", test_store1._data_list)
        #print ("local_list 2: ", test_store2._data_list)
        print("    check source list length after copy: handler 2")
        assert len(source_list) == 4
        


    #########################################################################
    # Test the mapping of the wml bundleresthandler
    # a specialized bundleresthandler class
    # - test preprocess() implementation: the mapping functionality
    #   for valid input tuples as well as invalid ones
    # - get the result and check it with expected
    #########################################################################
    def test_WmlBundleRestHandler_preprocess(self):

        print("############# test_WmlBundleRestHandler_preprocess() ###############")
        
        #functionally not needed in this testcase but needed for compilation
        class wml_client_stub ():
            class deployments_():
                def score(self,deployment_id, **meta_props):
                    if len(meta_props["meta_props"]["input_data"][0]["values"]) is 5:
                        return {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]]}]}
                    else:
                        return {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[6, 7], [7, 8], [9, 10]]}]}
            deployments = deployments_()      

        print("    ###### test with valid input only")
        # list of 10 valid tuples
        source_list = [{"a":i, "b": i+1, "c": i+2} for i in range(10)]

        ###################################################
        #initialize the handler class
        ###################################################
        #set base classes class variables
        WmlBundleRestHandler.max_copy_size = 5
        #lock = threading.Lock()
        lock = threading.Condition()
        WmlBundleRestHandler.input_list_lock = lock
        WmlBundleRestHandler.source_data_list = source_list
        WmlBundleRestHandler.single_output = False
        WmlBundleRestHandler.field_mapping=[{"model_field":"a_",
                                             "tuple_field":"a"},
                                            {"model_field":"b_",
                                             "tuple_field":"b"}]
        WmlBundleRestHandler.output_function = (lambda x: print("->->->", str( x)))
        #set WML sub classes class variables
        WmlBundleRestHandler.wml_client = wml_client_stub
        WmlBundleRestHandler.deployment = "deploymentid"
        
        test_store1 = WmlBundleRestHandler(1)
        test_store1.copy_from_source()
        test_store1.preprocess()
        expected_payload = [{'fields': ['a_', 'b_'], 'values': [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]]}]
        #print(test_store1.get_payload())
        print("    check expected payload: handler 1")
        assert expected_payload == test_store1.get_payload()
        
        print("    check source list length")
        assert len(source_list) == 5
        
        test_store2 = WmlBundleRestHandler(2)
        test_store2.copy_from_source()
        test_store2.preprocess()
        expected_payload = [{'fields': ['a_', 'b_'], 'values': [[5, 6], [6, 7], [7, 8], [8, 9], [9, 10]]}]
        #print(test_store2.get_payload())
        print("    check expected payload: handler 2")
        assert expected_payload == test_store2.get_payload()
        print("    check source list length")
        assert len(source_list) == 0
        


        print("    ####### test with valid and invalid input #######")
    
        # mixed list of 10 valid and invalid tuples
        source_list = [{"a":i, "b": i+1, "c": i+2} for i in range(10)]
        source_list[4].pop("b")
        source_list[2].pop("a")
        source_list[8].pop("b")
        source_list[8].pop("a")

        WmlBundleRestHandler.source_data_list = source_list

        test_store1 = WmlBundleRestHandler(1)
        test_store1.copy_from_source()
        test_store1.preprocess()
        expected_payload = [{'fields': ['a_', 'b_'], 'values': [[0, 1], [1, 2], [3, 4]]}]
        expected_status = [{'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': False, 'score_success': False, 'message': 'Mapping error: input field: a'}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': False, 'score_success': False, 'message': 'Mapping error: input field: b'}]
        #print(test_store1.get_payload())
        #print(test_store1.get_status())
        print("    check expected payload: handler 1")
        assert expected_payload == test_store1.get_payload()
        print("    check expected status: handler 1")
        assert expected_status == test_store1.get_status()
        
        print("    check source list length")
        assert len(source_list) == 5
        
        test_store2 = WmlBundleRestHandler(2)
        test_store2.copy_from_source()
        test_store2.preprocess()
        expected_payload = [{'fields': ['a_', 'b_'], 'values': [[5, 6], [6, 7], [7, 8], [9, 10]]}]
        expected_status = [{'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': False, 'score_success': False, 'message': 'Mapping error: input field: a'}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}]
        #print(test_store2.get_payload())
        #print(test_store2.get_status())
        print("    check expected payload: handler 2")
        assert expected_payload == test_store2.get_payload()
        print("    check expected status: handler 2")
        assert expected_status == test_store2.get_status()
        print("    check source list length")
        assert len(source_list) == 0


    #########################################################################
    # Test the WML bundleresthandler class and underlying base class
    #
    # Test the complete sequence of processing steps
    # - copy_from_source() -> don't check as tested before
    # - preprocess()       -> don't check as tested before
    # - sync_rest_call() -> use a WML client stub delivering certain result
    #                       use valid and invalid data tuples
    #                       check content of response delivered to handler
    # - postprocess()    -> check the generated results in result_list
    # - get_final_data() -> check the generated final tuple
    #                       original data extended with REST-result/error
    #########################################################################
    def test_WmlBundleRestHandler_synch_rest_call(self):

        print("############# test_WmlBundleRestHandler_synch_rest_call() ###############")

        # this test case needs a mockup of the wml api call wml_clien.deployments.score()
        # to be injected to class as loopback generating just what is expected
        class wml_client_stub ():
            class deployments_():
                def score(self,deployment_id, **meta_props):
                    if len(meta_props["meta_props"]["input_data"][0]["values"]) is 5:
                        return {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]]}]}
                    else:
                        return {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[6, 7], [7, 8], [9, 10]]}]}
            deployments = deployments_()      

        print("    ##### test with valid and invalid input ")
        # list of 10 tuples, 5 valid + 5 mixed
        source_list = [{"a":i, "b": i+1, "c": i+2} for i in range(10)]
        source_list[5].pop("a")
        source_list[8].pop("b")
        source_list[8].pop("a")

        ###################################################
        #initialize the handler class
        ###################################################
        #set handler base classes class variables
        WmlBundleRestHandler.max_copy_size = 5
        #lock = threading.Lock()
        lock = threading.Condition()
        WmlBundleRestHandler.input_list_lock = lock
        WmlBundleRestHandler.source_data_list = source_list
        WmlBundleRestHandler.single_output = False
        WmlBundleRestHandler.field_mapping=[{"model_field":"a_", "tuple_field":"a"},
                                            {"model_field":"b_", "tuple_field":"b"}]                                      
        WmlBundleRestHandler.output_function = output_class(self)
        #set WML handler sub classes class variables
        WmlBundleRestHandler.wml_client = wml_client_stub
        WmlBundleRestHandler.deployment_guid = "deploymentid"

        test_store1 = WmlBundleRestHandler(1)
        test_store1.copy_from_source()
        test_store1.preprocess()
        test_store1.synch_rest_call()
        
        expected_response = {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]]}]}
        #print(test_store1.get_rest_response())
        print("    check expected response: handler 1")
        assert expected_response == test_store1.get_rest_response()

        expected_status = [{'mapping_success': True, 'score_success': True, 'message': None}, 
                           {'mapping_success': True, 'score_success': True, 'message': None}, 
                           {'mapping_success': True, 'score_success': True, 'message': None}, 
                           {'mapping_success': True, 'score_success': True, 'message': None}, 
                           {'mapping_success': True, 'score_success': True, 'message': None}
                          ]
        #print(test_store1.get_status())
        print("    check expected status: handler 1")
        assert expected_status == test_store1.get_status()
        
        test_store1.postprocess()

        expected_result = [{'Prediction': {'prediction$1': 0, 'prediction$2': 1}}, 
                           {'Prediction': {'prediction$1': 1, 'prediction$2': 2}}, 
                           {'Prediction': {'prediction$1': 2, 'prediction$2': 3}}, 
                           {'Prediction': {'prediction$1': 3, 'prediction$2': 4}}, 
                           {'Prediction': {'prediction$1': 4, 'prediction$2': 5}}
                          ]
        #print(test_store1.get_postprocess_result())
        print("    check expected post process result: handler 1")
        assert expected_result == test_store1.get_postprocess_result()

        expected_final = [[{'Prediction': {'prediction$1': 0, 'prediction$2': 1},'a': 0, 'b': 1, 'c': 2}, 
                          {'Prediction': {'prediction$1': 1, 'prediction$2': 2},'a': 1, 'b': 2, 'c': 3}, 
                          {'Prediction': {'prediction$1': 2, 'prediction$2': 3},'a': 2, 'b': 3, 'c': 4}, 
                          {'Prediction': {'prediction$1': 3, 'prediction$2': 4},'a': 3, 'b': 4, 'c': 5}, 
                          {'Prediction': {'prediction$1': 4, 'prediction$2': 5},'a': 4, 'b': 5, 'c': 6}
                         ]]
        #print(test_store1.get_final_data())
        print("    check expected final result: handler 1")
        assert expected_final == test_store1.get_final_data()
        test_store1.write_result_to_output()
        
        print("    check source list length")
        assert len(source_list) == 5

        print ("    #####  test with valid and invalid input #########")
        # second bundle of size 5 with some errors
        test_store1 = WmlBundleRestHandler(1)
        test_store1.copy_from_source()
        test_store1.preprocess()
        test_store1.synch_rest_call()
        
        expected_response = {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 
                                              'values': [[6, 7], [7, 8], [9, 10]]}]}
        #print(test_store1.get_rest_response())
        print("    check expected response: handler 1")
        assert expected_response == test_store1.get_rest_response()

        expected_status = [{'mapping_success': False, 'score_success': False, 'message': 'Mapping error: input field: a'}, 
                           {'mapping_success': True, 'score_success': True, 'message': None}, 
                           {'mapping_success': True, 'score_success': True, 'message': None}, 
                           {'mapping_success': False, 'score_success': False, 'message': 'Mapping error: input field: a'}, 
                           {'mapping_success': True, 'score_success': True, 'message': None}
                          ]

        print("    check expected status: handler 1")
        #print(test_store1.get_status())
        assert expected_status == test_store1.get_status()
        
        test_store1.postprocess()

        expected_result = [{'PredictionError': 'Mapping error: input field: a'},
                           {'Prediction': {'prediction$1': 6, 'prediction$2': 7}}, 
                           {'Prediction': {'prediction$1': 7, 'prediction$2': 8}}, 
                           {'PredictionError': 'Mapping error: input field: a'},
                           {'Prediction': {'prediction$1': 9, 'prediction$2': 10}}
                          ]

        #print(test_store1.get_postprocess_result())
        print("    check expected post process result: handler 1")
        assert expected_result == test_store1.get_postprocess_result()

        expected_final = [[{'PredictionError': 'Mapping error: input field: a','b': 6, 'c': 7}, 
                          {'Prediction': {'prediction$1': 6, 'prediction$2': 7},'a': 6, 'b': 7, 'c': 8}, 
                          {'Prediction': {'prediction$1': 7, 'prediction$2': 8},'a': 7, 'b': 8, 'c': 9}, 
                          {'PredictionError': 'Mapping error: input field: a','c': 10}, 
                          {'Prediction': {'prediction$1': 9, 'prediction$2': 10},'a': 9, 'b': 10, 'c': 11}
                         ]]
        
        #print(test_store1.get_final_data())
        print("    check expected final result: handler 1")
        assert expected_final == test_store1.get_final_data()

        expected_success = [{'Prediction': {'prediction$1': 6, 'prediction$2': 7},'a': 6, 'b': 7, 'c': 8}, 
                            {'Prediction': {'prediction$1': 7, 'prediction$2': 8},'a': 7, 'b': 8, 'c': 9}, 
                            {'Prediction': {'prediction$1': 9, 'prediction$2': 10},'a': 9, 'b': 10, 'c': 11}
                           ]

        expected_error = [{'PredictionError': 'Mapping error: input field: a','b': 6, 'c': 7}, 
                          {'PredictionError': 'Mapping error: input field: a','c': 10} 
                         ]
        
        success,error = test_store1.get_final_data( single_list=False)
        print("    check expected stream data: handler 1")
        assert expected_success == success
        assert expected_error == error
        test_store1.write_result_to_output()
        

    def test_WmlBundleRestHandler_single_array_input(self):

        print("############# test_WmlBundleRestHandler_single_array_input() ###############")

        # this test case needs a mockup of the wml api call wml_clien.deployments.score()
        # to be injected to class as loopback generating just what is expected
        class wml_client_stub ():
            class deployments_():
                def score(self,deployment_id, **meta_props):
                    if len(meta_props["meta_props"]["input_data"][0]["values"]) is 5:
                        return {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]]}]}
                    else:
                        return {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[6, 7], [7, 8], [9, 10]]}]}
            deployments = deployments_()      

        # list of 10 tuples, 5 valid
        source_list = [{"a":numpy.array([i+1,i+2,i+3,i+4,i+5]), "b": i+1, "c": i+2} for i in range(10)]

        ###################################################
        #initialize the handler class
        ###################################################
        #set handler base classes class variables
        WmlBundleRestHandler.max_copy_size = 5
        lock = threading.Condition()
        WmlBundleRestHandler.input_list_lock = lock
        WmlBundleRestHandler.source_data_list = source_list
        WmlBundleRestHandler.single_output = False
        WmlBundleRestHandler.field_mapping=[{"model_field":"__array__", "tuple_field":"a"}]
        WmlBundleRestHandler.output_function = output_class(self)
        #set WML handler sub classes class variables
        WmlBundleRestHandler.wml_client = wml_client_stub
        WmlBundleRestHandler.deployment_guid = "deploymentid"


        print ("    ##### numpy.array handling #####")
                        
        test_store1 = WmlBundleRestHandler(1)
        test_store1.copy_from_source()
        test_store1.preprocess()
        expected_payload = [{'values': [[1, 2, 3, 4, 5], [ 2, 3, 4, 5, 6], [3, 4, 5, 6, 7], [ 4, 5, 6, 7, 8], [ 5, 6, 7, 8, 9]]}]
        print("    check expected payload: handler 1")
        #print(test_store1.get_payload())
        assert expected_payload == test_store1.get_payload()
        
        expected_status = [{'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}
                          ]
        print("    check expected status: handler 1")
        #print(test_store1.get_status())
        assert expected_status == test_store1.get_status()

    def test_WmlBundleRestHandler_single_list_input(self):

        print("############# test_WmlBundleRestHandler_single_list_input() ###############")

        # this test case needs a mockup of the wml api call wml_clien.deployments.score()
        # to be injected to class as loopback generating just what is expected
        class wml_client_stub ():
            class deployments_():
                def score(self,deployment_id, **meta_props):
                    if len(meta_props["meta_props"]["input_data"][0]["values"]) is 5:
                        return {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]]}]}
                    else:
                        return {'predictions': [{'fields': ['prediction$1', 'prediction$2'], 'values': [[6, 7], [7, 8], [9, 10]]}]}
            deployments = deployments_()      

        # list of 10 tuples, 5 valid
        source_list = [{"a":list([i+1,i+2,i+3,i+4,i+5]), "b": i+1, "c": i+2} for i in range(10)]

        ###################################################
        #initialize the handler class
        ###################################################
        #set handler base classes class variables
        WmlBundleRestHandler.max_copy_size = 5
        lock = threading.Condition()
        WmlBundleRestHandler.input_list_lock = lock
        WmlBundleRestHandler.source_data_list = source_list
        WmlBundleRestHandler.single_output = False
        WmlBundleRestHandler.field_mapping=[{"model_field":"__array__", "tuple_field":"a"}]
        WmlBundleRestHandler.output_function = output_class(self)
        #set WML handler sub classes class variables
        WmlBundleRestHandler.wml_client = wml_client_stub
        WmlBundleRestHandler.deployment_guid = "deploymentid"


        print ("    ##### list handling #####")
                        
        test_store1 = WmlBundleRestHandler(1)
        test_store1.copy_from_source()
        test_store1.preprocess()
        expected_payload = [{'values': [[1, 2, 3, 4, 5], [ 2, 3, 4, 5, 6], [3, 4, 5, 6, 7], [ 4, 5, 6, 7, 8], [ 5, 6, 7, 8, 9]]}]
        print("    check expected payload: handler 1")
        #print(test_store1.get_payload())
        assert expected_payload == test_store1.get_payload()
        
        expected_status = [{'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}, 
                           {'mapping_success': True, 'score_success': False, 'message': None}
                          ]
        print("    check expected status: handler 1")
        #print(test_store1.get_status())
        assert expected_status == test_store1.get_status()


        

class TestDistributed(Test):
    def setUp(self):
        # setup test config
        self.test_config = {}
        self.test_config[ConfigParams.SSL_VERIFY] = False  
        job_config = streamsx.topology.context.JobConfig(tracing='error')
        job_config.add(self.test_config)

    def _launch(self, topo):
        rc = streamsx.topology.context.submit('DISTRIBUTED', topo, self.test_config)
        print(str(rc))
        #if rc is not None:
            #if (rc.return_code == 0):
            #    rc.job.cancel()

class TestStreamingAnalytics(Test):
    def setUp(self):
        # setup test config
        self.test_config = {streamsx.topology.context.ConfigParams.SSL_VERIFY:False}
        self.test_config[ConfigParams.SSL_VERIFY] = False  
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)

    def _launch(self, topo):
        rc = streamsx.topology.context.submit('STREAMING_ANALYTICS_SERVICE', topo, self.test_config)
        print(str(rc))
        if rc is not None:
            if (rc.return_code == 0):
                rc.job.cancel()

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()
        super().setUpClass()

