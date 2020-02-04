from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.context import ConfigParams
import streamsx.topology.context
import streamsx.rest as sr
import unittest
import os
import json
import streamsx.wml as wml
import streamsx.wml.utils as wml_utils



#watson_machine_learning_client.WatsonMachineLearningAPIClient()

from watson_machine_learning_client import WatsonMachineLearningAPIClient
#test_func = wml_utils.get_wml_credentials()



def cloud_creds_env_var():
    result = {}
    try:
        result = os.environ['MACHINE_LEARNING_SERVICE_CREDENTIALS']
    except KeyError: 
        result = None
    return result




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


    def _create_stream(self, topo):
        """Create a stream of dicts, each having K/V for iris detection"""
        s = topo.source([{"sepal_length" : 5.1 , "sepal_width" : 3.5 , "petal_length" : 1.4, "petal_width" : 0.2, "tuple_number": i} for i in range (10000)])
        return s

    def test_score_bundle(self):
        print ('\n---------'+str(self))

        field_mapping =[{"model_field":"Sepal.Length",
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
                            "tuple_field":"petal_width"}]

        name = 'test_score_bundle'
        topo = Topology(name)
        source_stream = self._create_stream(topo) 
        # stream of dicts is consumed by wml_online_scoring
        scorings,invalids = wml.wml_online_scoring(source_stream,
                                     '72a15621-5e2e-44b5-a245-6a0fabc5de1e',#'c764e524-0876-4e03-a6da-5f3bbc5e5482', #deployment_guid
                                     field_mapping, 
                                     json.loads(cloud_creds_env_var()), #wml_credentials,
                                     'e34d2846-cc27-4e8a-80af-3d0f7021d0cb',#'1fb6550c-b22a-4a90-93fc-458ec048662e',
                                     expected_load = 1000,
                                     queue_size = 2000, 
                                     threads_per_node = 1)

        print_stream = scorings.map(lambda t: print(str(t)))

        scorings.publish(topic="ScoredRecords")
        invalids.publish(topic="InvalidRecords")

        #res.print()
        if (("TestDistributed" in str(self)) or ("TestStreamingAnalytics" in str(self))):
            self._launch(topo)
        else:
            # build only
            self._build_only(name, topo)

class TestDistributed(Test):
    def setUp(self):
        # setup test config
        self.test_config = {}
        self.test_config[ConfigParams.SSL_VERIFY] = False  
        job_config = streamsx.topology.context.JobConfig(tracing='trace')
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

