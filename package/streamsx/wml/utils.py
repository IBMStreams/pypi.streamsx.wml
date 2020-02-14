# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020

from watson_machine_learning_client import WatsonMachineLearningAPIClient


def get_wml_credentials(token=None,url=None,version="3.0.0"):
    """Returns a credential object to be used for WML connection
    
    Arguments:
    token -- token which should be included in the credentials
             this token has to be a non-expiring token as it is
             used at Streams runtime and no user credentials shall
             be needed resp. stored
             (default: users CPD token from Jupyter environment)
    url -- url of the CPD cluster where WML is running
    version -- version of the CPD cluster where WML is running
               type str, defaults to "3.0.0"
                 
    Call it only in the notebook where the topology is created, not in Python
    code executed at Streams runtime.
    """
    from icpd_core import icpd_util
        
    # take the token from notebook environment 
    if token is None:
        token = icpd_util.icpd_token
    if url is None:
        url = "https://internal-nginx-svc:12443"
    # credetials as to be used in CPD >=2.5
    credentials = {
                   "url": url, # access the main CP4D proxy/the CP4D cluster root from internal side
                   "token": token,
                   "instance_id": "wml_local",
                   "version" : version
                  }
    #test client creation to fail already in notebook if there is something wrong
    #need object copy here as the function changes "instance_id" in the object and so it 
    #couldn't be used anymore afterwards
    #wml_client = WatsonMachineLearningAPIClient(copy.copy(credentials))
    return credentials
    
def get_project_space(credentials):
    """Returns the notebooks project space GUID.
       
    Argument:
    credentials -- the credentials to be used to connect to WML
      
    Call it only in the notebook where the topology is created, not at Streams runtime.
    Models and deployments are placed in projects space if no other space is given at 
    their creation time.
    The space GUID is needed to instantiate a WMLOnlineScoring object.
    """
    from project_lib import Project
        
    wml_client = WatsonMachineLearningAPIClient(copy.copy(credentials))
    spaces = wml_client.spaces.get_details()["resources"]
    project = Project.access()
    project_guid = project.get_metadata()["metadata"]["guid"]
    # get the space associated with the project
    project_space=None
    for space in spaces:
        for tag in space["entity"]["tags"]:
            if tag["value"]=="dsx-project."+project_guid:
                project_space = space["metadata"]["id"]
    return project_space

