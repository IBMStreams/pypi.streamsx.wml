#
#*******************************************************************************
#* Copyright (C) 2018, International Business Machines Corporation. 
#* All Rights Reserved. *
#*******************************************************************************
#

# Import the SPL decorators
from streamsx.spl import spl
from streamsx.ec import get_application_configuration
from streamsx.ec import is_active
import re, os, time
import sys
import logging
import json
import time
from datetime import datetime
from watson_machine_learning_client.utils import MODEL_DETAILS_TYPE
from numpy.distutils.exec_command import temp_file_name
# WML specific imports
from watson_machine_learning_client import WatsonMachineLearningAPIClient
from watson_machine_learning_client.wml_client_error import ApiRequestFailure
from requests.exceptions import MissingSchema

import copy
import queue
import threading
import pickle



#define tracer and logger
#logger for error which should and can! be handled by an administrator
#tracer for all other events that are of interest for developer
tracer = logging.getLogger(__name__)
logger = logging.getLogger("com.ibm.streams.log")


# Defines the SPL namespace for any functions in this module
def spl_namespace():
    return "com.ibm.streams.wml"

@spl.primitive_operator(output_ports=['result_port','error_port'])
class WMLOnlineScoring(spl.PrimitiveOperator):
    """Providing the functionality to score incomming data with a model of any of the WML supported frameworks.
    The members __init__ and __call__ of this class will be called when topology application is submitted to the Streams runtime.
    So the thread of the runtime is the one putting the input tuple into the queue.
    
    It is designed to be used in a topology function to consume a stream of incoming tuples and 
    produce a stream of outgoing tuples with scoring results or in case of scoring errors a stream of
    tuples with error indication
    """
    def __init__(self, deployment_guid, 
                       #mapping_function, 
                       field_mapping,		
                       wml_credentials , 
                       space_guid, 
                       expected_load, 
                       queue_size, 
                       threads_per_node ):
        """Instantiates a WMLOnlineScoring object at application runtime (Streams application runtime container).
        
        It creates a WML client connecting to WML service with provided credentials and
        retrieves the URL of the deployment which should be used for scoring.        
        It creates the threads which handle the requests towards the scoring deployment.
        These threads will consume tuples in the input queue, which is filled by the __call__ member.
        """
        tracer.debug("__init__ called")
        self._deployment_guid = deployment_guid
        #self._mapping_function = mapping_function
        self._field_mapping = json.loads(field_mapping)
        self._wml_credentials = json.loads(wml_credentials)
        self._deployment_space = space_guid
        self._expected_load = expected_load
        self._max_queue_size = queue_size
        self._threads_per_node = threads_per_node
        self._node_count = 1
        self._max_request_size = 10 if expected_load is None else int(expected_load/self._threads_per_node/self._node_count)
        self._input_queue = list([])
        self._sending_threads = []
        self._lock = threading.Lock()
        self._thread_finish_counter = 0
        tracer.debug("__init__ finished")
        return

    def __enter__(self):
        tracer.debug("__enter__ called")
        self._create_sending_threads()
        self._wml_client = self._create_wml_client()
        tracer.debug("__enter__ finished")

    def all_ports_ready(self):
        tracer.debug("all_ports_ready() called")
        self._start_sending_threads()
        tracer.debug("all_port_ready() finished, sending threads started")
        return self._join_sending_threads()
    


    @spl.input_port()
    def score_call(self, **python_tuple):
        """It is called for every tuple of the input stream 
        The tuple will be just stored in the input queue. On max queue size processing
        stops and backpressure on the up-stream happens.
        """
        # Input is a single value python tuple. This value is the pickeled original tuple
        # from topology.
        # So we need to load it back in an object with pickle.load(<class byte>) from memoryview
        # we receive here as the pickled python object is put in a SPL tuple <blob __spl_po> and
        # SPL type blob is on Python side a memoryview object
        # python tuple is choosen as input type, which has tuple values in sequence of SPL tuple
        # we have control over this SPL tuple and define it to have single attribute being a blob 
        # the blob is filled from topology side with a python dict as we want to work on a dict
        # as most comfortable also when having no defined attribute sequence anymore
        input_tuple = pickle.loads(python_tuple['__spl_po'].tobytes())

        # allow backpressure, block calling thread here until input_tuple can be stored 
        while(len(self._input_queue) >=  self._max_queue_size):
            #todo check thread status
            time.sleep(1)
        with self._lock:
            #'Append' itself would not need a lock as from Python interpreter side it is
            #atomic, and Python threading is on Python level not C level.
            #But use lock here for the case of later added additional
            #code which has to be executed together with 'append'
            self._input_queue.append(input_tuple)


    def __exit__(self, exc_type, exc_value, traceback):
        tracer.debug("__exit__ called")
        self._end_sending_threads()
        tracer.debug("__exit__ finished, sending threads triggered to stop")
    
    
    def _rest_handler(self, thread_index):
        tracer.debug("Thread %d started.", thread_index )
        local_list = []
        tuple_counter = 0
        send_counter = 0
        #as long as thread shall not stop
        while self._sending_threads[thread_index]['run']:
            tracer.debug("Thread %d in loop received %d tuples.", thread_index,tuple_counter )
            #copy chunk of input tuples to threads own list and delete from global
            #our sending threads don't really need to run in parallel but in sequence so
            #we may lock for longer time here
            size = 0
            payload = []
            invalid_tuples = []
            predictions = []
            local_list = []
            with self._lock:
                #determine size and copy max size or all to local list
                size = len(self._input_queue)
                tracer.debug("WMLOnlineScoring: Thread %d input_queue len before copy %d!", thread_index, size)
                if size > 0:
                    end_index = int(self._max_request_size) if size >= self._max_request_size else size
                    local_list = self._input_queue[:end_index]
                    del self._input_queue[:end_index]
                    tuple_counter = tuple_counter + end_index 
                    tracer.debug("WMLOnlineScoring: Thread %d read %d tuples from input queue with local_list len %d!", thread_index, end_index, len(local_list))
                tracer.debug("WMLOnlineScoring: Thread %d input_queue len after copy %d!", thread_index, len(self._input_queue))
            
            #do the rest not in lock
            if size > 0: 
                #generate scoring payload and get back filtered invalid records
                payload, invalid_tuples = self._mapping_function(self._field_mapping,local_list)
                #send request
                try:
                    if len(payload) > 0:
                        predictions=self._wml_client.deployments.score(self._deployment_guid,meta_props={'input_data':payload})
                except wml_client_error.ApiRequestFailure as err:
                    """REST request returns 
                    400 incase something with the value of 'input_data' is not correct
                    404 if the deployment GUID doesn't exists as REST endpoint
                    
                    score() function throws in this case an wml_client_error.ApiRequestFailure exception
                    with two args: description [0] and the response [1]
                    use response.status_code, response.json()["errors"][0]["code"], response.json()["errors"][0]["message"]
                    
                    The complete payload is rejected in this case, no single element is referenced to be faulty
                    As such we need to write the complete payload to invalid_tuples being submitted to 
                    error output port
                    """
                    tracer.error("WML API error description: %s",str(err.args[0]))
                    logger.error("WMLOnlineScoring: WML API error: %s",str(err.args[0]))
                    #print("WML REST response headers: ",err.args[1].headers)
                    #print("WML REST response statuscode: ",err.args[1].status_code)
                    #print("WML REST response code: ",err.args[1].json()["errors"][0]["code"])
                    #print("WML REST response message: ",err.args[1].json()["errors"][0]["message"])
                    #add the complete local tuple list to invalid list
                    #TODO one may think about adding an error indicator if tuple is rejected from mapping function
                    #or from scoring as part of a scoring bundle
                    #because the predictioon for whole bundle failed, the complete local_list is invalid
                    #invalid_tuples += local_list
                    invalid_tuples = local_list
                    local_list = []
                except:
                    tracer.error("Unknown exception: %s", str(sys.exc_info()[0]))
                    logger.error("WMLOnlineScoring: Unknown exception: %s", str(sys.exc_info()[0]))
                    #because the predictioon for whole bundle failed, the complete local_list is invalid
                    #invalid_tuples += local_list
                    invalid_tuples = local_list
                    local_list = []

                tracer.debug("WMLOnlineScoring: Thread %d got %d predictions from WML model deployment!", thread_index, len(predictions['predictions'][0]['values']))
                 
                local_list_index = 0
                for prediction in predictions['predictions']:
                    #take the tuples from local list in sequence, sequence is same as the 
                    #sequence of prediction 'values' as input was generated in sequence of the local_list
                    #there is no reference from input to prediction except the position in sequence
                    #use output mapping function or just add the raw result to tuple
                    #for later separation and processing
                    #each prediction contains model result 'fields' and one or more 'values' lists
                    #one value list for each scoring set
                    #multiple predictions are only generated when the input contained multiple {'fields''values'} objects
                    #which happens when the model has optional parameters and input records doesn't have an optional
                    #field so new {'fields''values'} object with appropriate field names is generated as input

                    for values in prediction['values']:
                        #get a complete dict with field,value for each prediction result
                        prediction_dict = dict(zip(prediction['fields'],values))
                        # and add it to the stored tuple in local list
                        local_list[local_list_index]['prediction']=prediction_dict
                        #submit the default topology tuple for raw Python objects
                        self.submit('result_port',{'__spl_po':memoryview(pickle.dumps(local_list[local_list_index]))})
                        local_list_index +=1
                    send_counter += local_list_index
                    tracer.debug("WMLOnlineScoring: Thread %d submitted now % and %d in sum tuples",thread_index, local_list_index, send_counter)

                for _tuple in invalid_tuples:
                    #submit invalid tuples
                    tracer.debug("WMLOnlineScoring: Thread Submit %d invalid tuples",len(invalid_tuples))
                    #self.submit('error_port',invalid_tuples)
            else:
                #todo choose different approach to get threads waiting for input
                #may be queue with blocking read but queue we can't use to use subslicing and slice-deleting
                
                #thread should finish, once decremented the counter it shall no more run
                # and leave its task function
                #if self._thread_finish_counter > 0 :
                #    self._thread_finish_counter -= 1 
                #    self._sending_threads[thread_index]['run'] = False
                    
                time.sleep(0.5)
                
        tracer.info("WMLOnlineScoring: Thread %d stopped after %d records", thread_index,record_counter )
    
    
    def _create_wml_client(self):
        wml_client = WatsonMachineLearningAPIClient(self._wml_credentials)
        # set space before using any client function
        wml_client.set.default_space(self._deployment_space)
        return wml_client
    
    def _change_thread_number(self,delta):
        return

    def _change_deployment_node_number(self):
        return

    def _get_deployment_status(self):
        return
    
    def _determine_roundtrip_time(self):
        return
    
    def _create_sending_threads(self):
        for count in range(self._threads_per_node * self._node_count):
            tracer.debug("Create thread")
            thread_control = {'index':count,'run':True}
            thread_control['thread'] = threading.Thread(target = WMLOnlineScoring._rest_handler,args=(self,count))
            self._sending_threads.append(thread_control)
            tracer.debug("Thread data: %s",str(thread_control))
    
    def _start_sending_threads(self):
        for thread_control in self._sending_threads:
            tracer.debug("Start sending thread %s",str(thread_control))
            thread_control['thread'].start()
    
    def _end_sending_threads(self):
        for thread_control in self._sending_threads:
            thread_control['run'] = False
            
    def _join_sending_threads(self):
        tracer.debug("_join_sending_threads called during processing of operator stop.")
        
        # trigger threads to signal that they are ready
        # each will decrement by 1 if all are ready it's again 0
        #self._thread_finish_counter = len(self._sending_threads)
        #tracer.debug("Wait for %d threads to finish processing of buffers", len(self._sending_threads))
        
        # wait that the trigger becomes 0 and all threads left their task func
        #while self._thread_finish_counter > 0 : time.sleep(1.0)
        #tracer.debug("All threads finished processing of buffers")

        for thread_control in self._sending_threads:
            thread_control['thread'].join()
            tracer.debug("Thread %d joined.", thread_control['index'])

    def _mapping_function(self,model_field_mapping,tuple_list):
        """Private function, special for my model and my input data
        I have to know the fields the model requires and which I have to fill
        as well as the schema of my input data.
        
        Depending on the framework one need to provide the fields of the names or not.
        
        The required data format for scoring is a list of dicts containing "fields" and "values".
        "fields" is a list of fieldnames ordered as the model it requires
        "values" is a 2 dimensional list of multiple scoring data sets, where each set is a list of ordered field values 
        [{"fields": ['field1_name', 'field2_name', 'field3_name', 'field4_name'], 
        "values": [[value1, value2, value3, value4],[value1, value2,  value3, value4]]}]
        
        List of dicts with "fields" and "values" because a model may support optional fields. 
        If you want to add a tuple which doesn't have the same fields
        as the ones before and they are optional you need to add a new dict defining new 
        fields and add the values. As long as again tuples with other input
        field combination occurs for which you have to add again a new dict.
        
        !!!You need to know the required/optional fields of your model and check those in this mapping function.
        In case of invalid scoring input WML online scoring will reject the whole bundle with "invalid input" 
        reason without indicating which of the many inputs was wrong!!!
    
        !!!But those multiple field/values elements are not supported by all ML frameworks/runtimes
        SPARK runtime (used for SPARK ML and PMML models) doesn't support this
        """
        #this is a sample where all fields are required and are anytime in the input tuple
    
        #model fields in sequence as expected by the model
    
        invalid_tuples = []
        scoring_input = []
        actual_input_combination ={'fields':[],'values':[]}
        counter = 0
        for _tuple in tuple_list:
            counter += 1
            tuple_values = []
            tuple_fields = []
            invalid = False
            for field in model_field_mapping:
                if field['tuple_field'] in _tuple and _tuple[field['tuple_field']] is not None:
                    tuple_values.append(_tuple[field['tuple_field']])
                    tuple_fields.append(field['model_field'])
                    invalid = False
                else:
                    if field['is_mandatory']:
                        invalid_tuples.append(_tuple)
                        invalid = True
                        break
            if invalid == True: continue            
            if actual_input_combination['fields'] == tuple_fields:
                #same fields as before, just add further values
                actual_input_combination['values'].append(list(tuple_values))
            else:
                #close and store last fields/values combination in final scoring_input
                #except for the first valid being added
                if len(actual_input_combination['values']) > 0 : scoring_input.append(actual_input_combination) 
                #create new field/value combination
                actual_input_combination['fields']=tuple_fields
                actual_input_combination['values']=[list(tuple_values)]
                        
        #after last tuple store the open field/value combination in final scoring input
        scoring_input.append(actual_input_combination)
        return scoring_input, invalid_tuples
    










    # try to get the wml connection credential dictionary
    # "apikey","url","instance_id"
    # 1. check if input is json and has matching attributes
    #    "apikey","url","instance_id"
    # 2. check if input is Streams application configuration name
    #     2.1 check if all required single properties are provided
    #         "apikey","url","instance_id"
    #     2.2 check if "jsonCredentials" property is provided 
    #         and has all attributes 
    #         "apikey","url","instance_id"
    def _get_wml_connection_config(self, connection_config):
    
        #filter for dictionary, app config may include more parameter than we need
        wml_keys = ["apikey","url","instance_id"]
        wml_json_key = "jsonCredentials"
        config_dict=None
        

        if not connection_config:
            return None


        # 1. check if input is a JSON string
        tracer.info("Check if parameter connectionConfiguration is a JSON string")
        try: 
            config_dict = json.loads(connection_config)
        except:
            config_dict = None
           
        # valid JSON!
        if config_dict:
            tracer.info("Parameter connectionConfiguration is a JSON string, check content")
            if all (k in config_dict for k in wml_keys):
                #check that all have values
                if all (config_dict[k]!=None for k in wml_keys):
                    return {k:config_dict[k] for k in wml_keys}            
                    
        # no valid JSON,  possibly applicationConfiguration?
        else:
            tracer.info("parameter connectionConfiguration is no JSON string, check if it is AppConfig name")
            config_dict=get_application_configuration(connection_config)
            
            if config_dict:
                # 2.1 check all single properties provided
                tracer.info("Try to read separate credential parameters from AppConfig")
                if all (k in config_dict for k in wml_keys):
                    #check that all have values
                    if all (config_dict[k]!=None for k in wml_keys):
                        return {k:config_dict[k] for k in wml_keys}
                        
                # 2.2 check on jsonCredentials property
                tracer.info("Try to read single jsonCredentials parameter from AppConfig")
                if wml_json_key in config_dict:
                    json_config = None
                    try: 
                        json_config = json.loads(config_dict[wml_json_key])
                    except:
                        tracer.info("Could not load jsonCredentials from AppConfig")
                        json_config = None
                    if json_config:
                        if all (k in json_config for k in wml_keys):
                            #check that all have values
                            if all (json_config[k]!=None for k in wml_keys):
                                return {k:json_config[k] for k in wml_keys}            
                                
        tracer.error("WMLModelFeed: Could not get valid WML connection config from: " + config_name + ".")
        return None



