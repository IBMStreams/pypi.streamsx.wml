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
from bundleresthandler.wmlbundleresthandler import WmlBundleRestHandler
# WML specific imports
from watson_machine_learning_client import WatsonMachineLearningAPIClient
from watson_machine_learning_client.wml_client_error import ApiRequestFailure
# standard python imports
import re, os
import sys
import logging
import json
import time
from datetime import datetime
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


# callable class just to be given as output handler
class output_class():
    def __init__(self, output_object):
        self._output_object = output_object
    def __call__(self, results):
        #with self._output_object._output_lock:
        tracer.debug("Start output_function")
        for index,result_list in enumerate(results):
            tracer.debug("Start result submission ")
            if index == 0:
                for list_element in result_list:
                    self._output_object.submit('result_port',{'__spl_po':memoryview(pickle.dumps(list_element))})
            elif index == 1:
                if not self._output_object._single_output:
                    for list_element in result_list:
                        self._output_object.submit('error_port',{'__spl_po':memoryview(pickle.dumps(list_element))})
                else:
                    tracer.error("Internal error: Single output configured but error_list generated. ")
            else:
                tracer.error("Internal error: More result lists generated than supported. ")



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
                       threads_per_node,
                       single_output,
                       node_count):
        """Instantiates a WMLOnlineScoring object at application runtime (Streams application runtime container).
        
        It creates a WML client connecting to WML service with provided credentials and
        retrieves the URL of the deployment which should be used for scoring.        
        It creates the threads which handle the requests towards the scoring deployment.
        These threads will consume tuples in the input queue, which is filled by the __call__ member.
        """
        tracer.debug("__init__ called")
        self._deployment_guid = deployment_guid
        # self._mapping_function = mapping_function
        self._field_mapping = json.loads(field_mapping)
        self._wml_credentials = json.loads(wml_credentials)
        self._deployment_space = space_guid
        self._expected_load = expected_load
        self._max_queue_size = queue_size
        self._threads_per_node = threads_per_node
        self._single_output = single_output
        self._node_count = node_count
        self._max_request_size = 10 if expected_load is None else int(expected_load/self._threads_per_node/self._node_count)
        self._input_queue = list([])
        self._sending_threads = []
        self._lock = threading.Lock()
        self._output_lock = threading.Lock()
        self._thread_finish_counter = 0
        
        # Configure the WmlBundleRestHandler class
        WmlBundleRestHandler.max_copy_size = self._max_request_size
        WmlBundleRestHandler.input_list_lock = self._lock
        WmlBundleRestHandler.source_data_list = self._input_queue
        WmlBundleRestHandler.field_mapping = self._field_mapping
        WmlBundleRestHandler.output_function = output_class(self) 
        WmlBundleRestHandler.single_output = self._single_output
        
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

        # force backpressure, block calling thread here until input_tuple can be stored 
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
        
        wml_bundle_handler = WmlBundleRestHandler(thread_index,self._wml_client,self._deployment_guid)
        tuple_counter = 0
        send_counter = 0
        #as long as thread shall not stop
        while self._sending_threads[thread_index]['run']:
            #tracer.debug("Thread %d in loop received %d tuples.", thread_index,tuple_counter )
            if  wml_bundle_handler.copy_from_source() > 0:
                wml_bundle_handler.preprocess()
                wml_bundle_handler.synch_rest_call()
                wml_bundle_handler.postprocess()
                wml_bundle_handler.write_result_to_output()
            else:
                #todo choose different approach to get threads waiting for input
                #may be queue with blocking read but queue we can't use to use subslicing and slice-deleting
                
                #thread should finish, once decremented the counter it shall no more run
                # and leave its task function
                #if self._thread_finish_counter > 0 :
                #    self._thread_finish_counter -= 1 
                #    self._sending_threads[thread_index]['run'] = False
                    
                time.sleep(0.2)
                
        tracer.info("WMLOnlineScoring: Thread %d stopped after %d records", thread_index,record_counter )
            
    
    
    def _create_wml_client(self):
        wml_client = WatsonMachineLearningAPIClient(self._wml_credentials)
        # set space before using any client function
        wml_client.set.default_space(self._deployment_space)
        tracer.debug("WMLOnlineScoring: WML client created")
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


