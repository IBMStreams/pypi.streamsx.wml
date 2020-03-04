#
#*******************************************************************************
#* Copyright (C) 2018, International Business Machines Corporation. 
#* All Rights Reserved. *
#*******************************************************************************
#
# WML specific imports
from watson_machine_learning_client import WatsonMachineLearningAPIClient
# REST handler specific imports
from .bundleresthandler import BundleRestHandler
# standard python imports
import logging
import json
import time
import threading
import pickle
import sys

release = False

#define tracer and logger
#logger for error which should and can! be handled by an administrator
#tracer for all other events that are of interest for developer
tracer = logging.getLogger(__name__)
if not release:
    tracer.setLevel(logging.DEBUG)
    tracer.addHandler(logging.StreamHandler(sys.stdout))
    
logger = logging.getLogger("com.ibm.streams.log")


class BundleController():
    """
    
    """
    def __init__(self, 
                       expected_load = None, 
                       queue_size = None, 
                       threads_per_node = None,
                       single_output = None,
                       node_count = None,
                       handler_class = None,
                       field_mapping = None,
                       output_function = None
                      ):

        tracer.debug("__init__ called")

        #############################################################
        # Parameters
        ############################################################
        self._expected_load = expected_load
        self._max_queue_size = queue_size
        self._threads_per_node = threads_per_node
        self._single_output = single_output
        self._node_count = node_count
        self._max_request_size = 10 if expected_load is None else int(expected_load/self._threads_per_node/self._node_count)
        self._handler_class = handler_class

        
        ############################################################
        # internal variables
        ############################################################
        self._input_queue = list([])
        self._sending_threads = []
        self._lock = threading.Condition() # changed to condition
        self._output_lock = threading.Lock()
        self._thread_finish_counter = 0

        
        assert(self._handler_class is not None)
        ############################################################
        # Configure the handler class to be used
        # with the handler base classes class parameters 
        ############################################################
        self._handler_class.max_copy_size = self._max_request_size
        self._handler_class.input_list_lock = self._lock
        self._handler_class.source_data_list = self._input_queue
        self._handler_class.single_output = self._single_output
        self._handler_class.field_mapping = json.loads(field_mapping)
        self._handler_class.output_function = output_function

        tracer.debug("__init__ finished")
        return
        
    def process_data(self, input_data):
        """It is called for every single input data
        It will be just stored in the input queue. On max queue size processing
        blocks and backpressure on the up-stream/sending_thread happens.
        """
        with self._lock:
            self._lock.wait_for(lambda : len(self._input_queue) <= self._max_queue_size)
            self._input_queue.append(input_data)
            self._lock.notify()


    def prepare(self):
        self._create_sending_threads()
    
    def run(self):
        self._start_sending_threads()
        
    def stop(self):
        self._end_sending_threads()

    def finish(self):
        self._join_sending_threads()

    def _change_thread_number(self,delta):
        return

   
    def _create_sending_threads(self):
        for count in range(self._threads_per_node * self._node_count):
            tracer.debug("Create thread")
            handler_instance = self._handler_class(count)
            thread_control = {'index':count,'run':True, 'handler':handler_instance}
            thread_control['thread'] = threading.Thread(target = handler_instance.run)
            self._sending_threads.append(thread_control)
            tracer.debug("Thread data: %s",str(thread_control))

    
    def _start_sending_threads(self):
        for thread_control in self._sending_threads:
            tracer.debug("Start sending thread %s",str(thread_control))
            thread_control['thread'].start()
    
    def _end_sending_threads(self):
        for thread_control in self._sending_threads:
            #thread_control['run'] = False
            thread_control['handler'].stop()
            
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


