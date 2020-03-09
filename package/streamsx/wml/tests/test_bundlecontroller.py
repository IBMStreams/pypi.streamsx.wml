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
from streamsx.wml.bundleresthandler.bundlecontroller import BundleController
from streamsx.wml.bundleresthandler.wmlbundlecontroller import WmlBundleController

import threading




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



    ##################################################################
    # Test the interface for creating, starting, stopping, finishing
    # bundleresthandler threads.
    #
    # Only bundleresthandler base class is initialized
    # Only base class provided functions are called here as no data
    # is injected the handlers processing sequence is not started.
    ##################################################################        
    def test_bundleController_simplethreads(self):
        print("############# test_bundleController_simplethreads() ###############")
        field_mapping =json.dumps([{"model_field":"Sepal.Length",
                            "is_mandatory":True,
                            "tuple_field":"sepal_length"},
                           {"model_field":"Sepal.Width",
                            "is_mandatory":True,
                            "tuple_field":"sepal_width"},
                           {"model_field":"Petal.Length",
                            "is_mandatory":True,
                            "tuple_field":"petal_length"},
                           {"model_field":"Petal.Width",
                            "is_mandatory":True,
                            "tuple_field":"petal_width"}])


        client = BundleController (
                       ###################################
                       # only base class arguments
                       ###################################
                       expected_load = 10, 
                       queue_size = 100, 
                       threads_per_node = 3,
                       single_output = False,
                       node_count = 1,
                       field_mapping = field_mapping,
                       output_function = (lambda x: print(x)),
                       handler_class = BundleRestHandler)
        client.prepare()
        time.sleep(2)
        client.run()
        time.sleep(2)
        client.stop()
        time.sleep(2)
        client.finish()
        
        print ("    Test OK")


    ##################################################################
    # Test the interface for creating, starting, stopping, finishing
    # wmlbundleresthandler threads
    #
    # wmlbundleresthandler and its base class are initialized
    # Only base class provided functions are called here as no data
    # is injected the handlers processing sequence is not started.
    ##################################################################        
    def test_WmlBundleController_simplethreads(self):
        print("############# test_WmlBundleController_simplethreads() ###############")

        field_mapping =json.dumps([{"model_field":"Sepal.Length",
                            "is_mandatory":True,
                            "tuple_field":"sepal_length"},
                           {"model_field":"Sepal.Width",
                            "is_mandatory":True,
                            "tuple_field":"sepal_width"},
                           {"model_field":"Petal.Length",
                            "is_mandatory":True,
                            "tuple_field":"petal_length"},
                           {"model_field":"Petal.Width",
                            "is_mandatory":True,
                            "tuple_field":"petal_width"}])


        client = WmlBundleController (
                       ###################################
                       # WML sub class specific arguments
                       ###################################
                       deployment_guid = 'xyz', 
                       wml_credentials = cloud_creds_env_var(), 
                       space_guid = 'xyz', 
                       ###################################
                       # base class arguments
                       ###################################
                       expected_load = 10, 
                       queue_size = 100, 
                       threads_per_node = 3,
                       single_output = False,
                       node_count = 1,
                       field_mapping = field_mapping,		
                       handler_class = WmlBundleRestHandler
                       )
                       
        client.prepare()
        time.sleep(2)
        client.run()
        time.sleep(2)
        client.stop()
        time.sleep(2)
        client.finish()


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

