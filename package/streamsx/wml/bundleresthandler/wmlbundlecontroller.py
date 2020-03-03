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

