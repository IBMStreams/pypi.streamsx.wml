# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Changes
+++++++

v1.0.3:  
- documentation updates: links in description updated

v1.0.1:     
- deprecate: parameter expected_load, but will be supported for backward 
             compatibility if new parameter bundle_size is not provided
- new: parameter bundles_size determines the bundle size directly


Overview
++++++++

Provides functions to use Watson Machine Learning (WML) online scoring in `topology based streaming applications <https://www.ibm.com/support/knowledgecenter/SSQNUZ_3.0.1/cpd/svc/streams/developing-intro.html>`_.
All models which are supported by WML for online scoring can be used to score streaming data in a topology application.
The models have to be created with tools provided by WML and Cloud Pak for Data. They need to be stored in WML repository 
and published as an online deployment. 


This package is designed for high tuple throughput. It is using the WML feature of sending multiple input data 
within a single scoring request to the online scoring REST endpoint. This minimizes the communication overhead. 
On the other side it will increase latency, but only in milli second region.


Quick ref to use this package
+++++++++++++++++++++++++++++

1. Create a model and store it in CP4D WML repository
   There are good tutorials available for the different supported ML frameworks.
   To be able to store a model in CP4D WML repository you need to create a 'deployment space'.
   You can create different spaces for different scenarioa (test, dev, production). Depending on 
   the tool you are using to create and store your model you will be asked to choose or create a 
   'deployment space'. In Jupyter notebook you need to assign the 'space id' to your created WML client.
   You can also assign a space to your project, which can now be used as 'deployment space' in the notebook.
   
       `CPD 3.0.1: Create and store models <https://www.ibm.com/support/knowledgecenter/SSQNUZ_3.0.1/wsj/analyze-data/ml-notebook_local.html>`_
   

2. Create a WML online scoring deployment
   You create a deployment by choosing a stored model ID and the type of deployment. Again you can do it 
   via CP4D GUI as context specific action on a shown model or via Python API e.g. in a notebook.
   You will get information about the progress and the final state of the deployment creation.
   An WML online scoring deployment is a REST endpoint to which you send (https request) your input data and get scoring results
   back as response.
   
       `CPD 3.0.1: Store and deploy models <https://www.ibm.com/support/knowledgecenter/SSQNUZ_3.0.1/wsj/wmls/wmls-deploy-python.html>`_
   

3. Create the streaming application
   Once the deployment is active you can create the streaming application using the streamsx Python package. 
   You can read data from several sources, transform them, do analytics and write them sinks. In this process you
   can integrate now the streamsx.wml.wml_online_scoring() function which provides the high performance approach of
   WML online scoring. Which means that multiple input data are send within a single REST request to the scoring REST
   endpoint. The maximum number of contained input data can be controlled by parameter.
   
       `CPD 3.0.1: Analyze streaming data <https://www.ibm.com/support/knowledgecenter/SSQNUZ_3.0.1/cpd/svc/streams/developing-intro.html>`_


Sample
++++++

A simple sample which scores on an online deployment running a model created for the well know IRIS data 
set to predict the Iris species from size of the petal and sepal of an exemplar::

    from icpd_core import icpd_util
    from streamsx.topology.topology import Topology
    from streamsx.topology import context
    import json
    import time
    import streamsx.wml as wml
    import streamsx.wml.utils as wml_utils
    
    streams_instance_name = 'streams'
    cfg=icpd_util.get_service_instance_details(name=streams_instance_name, instance_type="streams")
    
    #field mapping supports JSON string and Python dict
    field_mapping_dict =[{"model_field":"Sepal.Length",
                          "tuple_field":"sepal_length"},
                         {"model_field":"Sepal.Width",
                          "tuple_field":"sepal_width"},
                         {"model_field":"Petal.Length",
                          "tuple_field":"petal_length"},
                         {"model_field":"Petal.Width",
                          "tuple_field":"petal_width"}]
    
    #field mapping can be of type dict or a JSON string reflecting same content
    field_mapping = json.dumps(field_mapping_dict)
    
    # credentials support JSON string or Python dict
    logged_in_users_wml_credentials = json.dumps(wml_utils.get_wml_credentials())  #token,url,instance_id,version
    
    topo = Topology(name="WMLOnlineScoring")
    
    class TestSource:
        def __init__(self, ):
            pass
        def __call__(self):
            # let it stream forever
            counter = 0
            while True:
                counter += 1
                time.sleep(0.01)
                #yield everytime same values, doesn't matter for test
                yield {"petal_length":1.4,
                       "petal_width":0.2,
                       "sepal_length":5.1,
                       "sepal_width":3.5,
                       "number" : counter}
    
    records = topo.source(TestSource())
    
    # 2 result streams are generated: 1st with successful scorings, 2nd with failed scorings because of invalid input
    scorings,invalids = wml.wml_online_scoring(records, #input stream
                                         '72a15621-5e2e-44b5-a245-6a0fabc5de1e', #deployment_guid
                                         field_mapping, 
                                         logged_in_users_wml_credentials, 
                                         'e34d2846-cc27-4e8a-80af-3d0f7021d0cb', #space_guid
                                         bundle_size = 10,
                                         queue_size = 1000, 
                                         threads_per_node = 1)
    
    # publish results as JSON
    scorings.publish(topic="ScoredRecords",schema=json,name="PublishScores")
    score_view = scorings.view(name="ScoredRecords", description="Sample of scored records")
    
    # Disable SSL certificate verification on test
    cfg[context.ConfigParams.SSL_VERIFY] = False
    
    # build and submit
    submission_result = context.submit('DISTRIBUTED', 
                                       topo, 
                                       cfg)

"""


__version__='1.0.3a0'

__all__ = ['wml_online_scoring']
from streamsx.wml._wml import wml_online_scoring

