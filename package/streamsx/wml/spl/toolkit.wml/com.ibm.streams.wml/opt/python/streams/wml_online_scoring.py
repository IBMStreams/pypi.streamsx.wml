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
from bundleresthandler.wmlbundlecontroller import WmlBundleController
# standard python imports
import re, os
import sys
import logging
import json
import time
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




######################################################
# callable class just to be given as output handler
# taking the result list of handler and using
# SPL operators submit() function.
# It is storing the reference to the SPL operator
# to reach the correct submit().
######################################################
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
                for list_element in result_list:
                    self._output_object.submit('error_port',{'__spl_po':memoryview(pickle.dumps(list_element))})
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

        #self._output_lock = threading.Lock()
       
        # Configure the specific controller class to be used
        self._controller = WmlBundleController(
                        # controller base class arguments
                        expected_load = expected_load, 
                        queue_size = queue_size, 
                        threads_per_node = threads_per_node,
                        single_output = single_output,
                        node_count = node_count,
                        field_mapping = field_mapping,
                        output_function = output_class(self),
                        # wml specific controler argumnets
                        deployment_guid = deployment_guid, 
                        wml_credentials = wml_credentials, 
                        space_guid = space_guid
                        ) 


        # type of input stream, set with first received tuple
        self._is_python_object_stream = None

        tracer.debug("__init__ finished")
        return
        

    def __enter__(self):
        tracer.debug("__enter__ called")
        self._controller.prepare()
        tracer.debug("__enter__ finished")

    def all_ports_ready(self):
        tracer.debug("all_ports_ready() called")
        self._controller.run()
        tracer.debug("all_port_ready() finished, sending threads started")
        return self._controller.finish()
    


    @spl.input_port()
    def process(self, **python_tuple):
        """It is called for every tuple of the input stream.
        The tuple will be given to a controller which may
        block and force backpressure on the up-stream.
        """
        # python_tuple is either a k/v element with key '__spl_po' indicating that the content v is a pickled Python dictionary.
        # Or python_tuple is a dictionary containing the k/v representation of a structured schema
        # (nameTuple or SPL schema). 
        # So we need to load it back in an object with pickle.load(<class byte>) from memoryview
        # we receive here as the pickled python object is put in a SPL tuple <blob __spl_po> and
        # SPL type blob is on Python side a memoryview object
        # python tuple is choosen as input type, which has tuple values in sequence of SPL tuple
        # we have control over this SPL tuple and define it to have single attribute being a blob 
        # the blob is filled from topology side with a python dict as we want to work on a dict
        # as most comfortable also when having no defined attribute sequence anymore

        input_tuple=None
        if self._is_python_object_stream is True:
            input_tuple = pickle.loads(python_tuple['__spl_po'].tobytes())
        elif self._is_python_object_stream is False:
            input_tuple = python_tuple
        # only entered at first tuple, next time only the above two are checked
        elif "__spl_po" in python_tuple:
            input_tuple = pickle.loads(python_tuple['__spl_po'].tobytes())
            self._is_python_object_stream = True
        else:
            input_tuple = python_tuple
            self._is_python_object_stream = False
            
        # controller.process can block calling thread here until input_tuple can be stored 
        self._controller.process_data(input_tuple)


    def __exit__(self, exc_type, exc_value, traceback):
        tracer.debug("__exit__ called")
        self._controller.end()
        tracer.debug("__exit__ finished, triggered all sub threads to stop")
    
    
            
    
    
    
    


