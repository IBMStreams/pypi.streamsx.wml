#
#*******************************************************************************
#* Copyright (C) 2018, International Business Machines Corporation. 
#* All Rights Reserved. *
#*******************************************************************************

# Bundle
from .wmlbundleresthandler import WmlBundleRestHandler
from .bundlecontroller import BundleController

# WML specific imports
from watson_machine_learning_client import WatsonMachineLearningAPIClient

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


class WmlBundleController(BundleController):
    def __init__(self, deployment_guid = None, 
                       wml_credentials = None, 
                       space_guid = None, 
                       **kwargs
                       ):

        tracer.debug("__init__ called")
        
        ######################################################
        # initialize the controller base class with arguments 
        # and add the handler class to be used
        ######################################################
        kwargs["handler_class"] = WmlBundleRestHandler
        super().__init__(**kwargs)
        ######################################################
        # initialize this controller sub class with its own
        # special arguments
        ######################################################
        self._deployment_guid = deployment_guid
        self._wml_credentials = json.loads(wml_credentials)
        self._deployment_space = space_guid
        ######################################################
        # set specialized handler class class variables 
        # we know at this place which specialized class
        # we use as we set it above
        # the handler base class class variables are set by
        # our own base class
        ######################################################
        #tracer.debug("Handler class: ", self._handler_class)
        self._handler_class.wml_client = self._create_wml_client()
        self._handler_class.deployment_guid = self._deployment_guid 

        tracer.debug("__init__ finished")
        return

    '''    
    ##########################################################
    # Streams specific function
    # necessary to use the WmlThreadedRestHandler class
    # as a Streams operator
    ##########################################################
    def __enter__(self):
        tracer.debug("__enter__ called")
        self._create_sending_threads()
        self._wml_client = self._create_wml_client()
        BundleRestHandlerClass._wml_client = self._wml_client
        tracer.debug("__enter__ finished")

    ##########################################################
    # Streams specific function
    # necessary to use the WmlThreadedRestHandler class
    # as a Streams operator
    ##########################################################
    def all_ports_ready(self):
        tracer.debug("all_ports_ready() called")
        self._start_sending_threads()
        tracer.debug("all_port_ready() finished, sending threads started")
        return self._join_sending_threads()
    

    ########################################################
    # function being called by a data generating thread to
    # put data into Rest processing
    ########################################################
    def ingest_data(self, **python_tuple):
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
        
        # super store data will 
        self.store_data(input_tuple)

    ##########################################################
    # Streams specific function
    # necessary to use the WmlThreadedRestHandler class
    # as a Streams operator
    ##########################################################
    def __exit__(self, exc_type, exc_value, traceback):
        tracer.debug("__exit__ called")
        self._end_sending_threads()
        tracer.debug("__exit__ finished, sending threads triggered to stop")
    '''    

    def _change_deployment_node_number(self):
        return

    def _get_deployment_status(self):
        return

    
    def _create_wml_client(self):
        tracer.debug("Creating WML client")
        wml_client = WatsonMachineLearningAPIClient(self._wml_credentials)
        # set space before using any client function
        wml_client.set.default_space(self._deployment_space)
        tracer.debug("WML client created")
        return wml_client

