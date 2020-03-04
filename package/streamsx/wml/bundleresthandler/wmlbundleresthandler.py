# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
   
   
from watson_machine_learning_client import WatsonMachineLearningAPIClient
from watson_machine_learning_client.wml_client_error import ApiRequestFailure

import logging
import numpy   
tracer = logging.getLogger(__name__)   
logger = logging.getLogger("com.ibm.streams.log")   

from .bundleresthandler import BundleRestHandler   
   
   
_STREAMSX_MAPPING_ERROR_ = "Mapping error: "
   
class WmlBundleRestHandler(BundleRestHandler):

    ########################################
    # public controllable class varibales
    ########################################
    wml_client = None
    deployment_guid = None
        
        
    def __init__(self,handler_index):
        super().__init__(handler_index)

        
    def preprocess(self):
        """WML specific implementation,
        One has to know the fields the model requires as well as the schema of input data.
        Those data is defined through the mapping configuration, set as class variable
        by outer application.
        
        Depending on the framework one need to provide the fields of the names or not.
        
        The payload format for WML scoring is a list of dicts containing "fields" and "values".
        "fields" is a list of fieldnames ordered as the model it requires
        "values" is a 2 dimensional list of multiple scoring data sets, where each set is a list of ordered field values 
        [{"fields": ['field1_name', 'field2_name', 'field3_name', 'field4_name'], 
        "values": [[value1, value2, value3, value4],[value1, value2,  value3, value4]]}]

        In case of a single_array_only input there may be no "fields" infomation needed, as the position in array 
        is sufficient.
        
        To indicate from application side that there is no field mapping necessary, the special mapping
        element [{"model_field":"__array__","tuple_field":"<name_of_tuple_field>"}]  is defined.
        This mapping has to be the only one, additional other mapping entries are not valid.
        The type of the tuple_field can be List, numpy.ndarray or pandas.DataFrame. 
        numpy.ndarray and pandas.DataFrame have to be converted to List. (may be changed in future 
        so that the wml client will do conversation.)
        Actually our implementation has to take care for a single line numpy.ndarray as scoring input.
        
        We don't support:
        - multi dimensional ndarray as scoring input for a single tuple
        - pandas.DataFrame as scoring for a single tuple
          DF are used commonly as column labeled two dimensional tabular data, which doesn't match
          single tuple input scenario but batch input scenario which we don't support.
        
        In case of invalid scoring input WML online scoring will reject the whole bundle with "invalid input" 
        reason without indicating which of the many inputs was wrong!!!
        """
        # this is a sample where all fields are required and are anytime in the input tuple
        # model fields have to be in order/sequence as expected by the model

        # keep this assert as long as we don't support optional fields
        assert self.allow_optional_fields is False
        
        # clear payload list
        self._payload_list = []      
        actual_input_combination ={'fields':[],'values':[]}
        
        for index,_tuple in enumerate(self._data_list):
            ###################################
            # field names and values from tuple
            # handling fields names is already preparation
            # for optional field support and resulting
            # different payload content
            ###################################
            tuple_values = []
            tuple_fields = []
            tuple_error = ""
            tuple_is_valid = True
            for field in self.field_mapping:
                if field['tuple_field'] in _tuple and _tuple[field['tuple_field']] is not None:
                    tuple_field_value = _tuple[field['tuple_field']]
                    # Handle the case that a single numpy array / List is the scoring input 
                    # special mapping '__array__'has to be the only mapping
                    # no field name used, ndarray needs 
                    if field['model_field'] == '__array__':
                        if isinstance(tuple_field_value, numpy.ndarray) and len(self.field_mapping) is 1:
                            tuple_values = list(tuple_field_value)
                            break
                        elif isinstance(tuple_field_value, List)  and len(self.field_mapping) is 1:
                            tuple_values = tuple_field_value
                            break
                        else:
                            tuple_is_valid = False
                            break
                    # the normal case, multiple mapping entries and using field name 
                    # tuple field value
                    else:
                        tuple_values.append(tuple_field_value)
                        tuple_fields.append(field['model_field'])

                #########################################################
                # optional fields are actually not supported
                # uncomment next prepared code lines in case we support 
                # it in all code parts
                #########################################################
                #elif self.allow_optional_fields:
                #    if field['is_mandatory']:
                #        tuple_is_valid = False
                #        break
                
                else:
                    tuple_is_valid = False
                    break

            if not tuple_is_valid:            
                tuple_error = "input field: " + field['tuple_field']

            ######################
            # store tuple status
            ######################
            if tuple_is_valid: 
                self._status_list[index]["mapping_success"] = True
            else:
                self._status_list[index]["mapping_success"] = False
                self._status_list[index]["message"] = _STREAMSX_MAPPING_ERROR_ + tuple_error
                continue            

            
            #######################
            # add payload for tuple
            #######################    
            if actual_input_combination['fields'] == tuple_fields:
                #same fields as before, just add further values
                actual_input_combination['values'].append(list(tuple_values))
            elif len(actual_input_combination['values']) is 0:  
                # first tuple
                actual_input_combination['fields']=tuple_fields
                actual_input_combination['values']=[list(tuple_values)]
            else:
                tuple_is_valid = False
                tuple_error = " optional fields are not allowed"
                # following code is preparation for support of multiple field combinations
                # in case of supported optional fields
                #
                #close and store last fields/values combination in final _payload_list
                #except for the first valid tuple being added
                #if len(actual_input_combination['values']) > 0 : self._payload_list.append(actual_input_combination) 
                ##create new field/value combination
                #actual_input_combination['fields']=tuple_fields
                #actual_input_combination['values']=[list(tuple_values)]

                        
        #after last tuple store the open field/value combination finally in payload_list
        if len(actual_input_combination['values']) > 0:
            # remove empty fields element resulting from single array as input 
            if len(actual_input_combination['fields']) is 0:
                actual_input_combination.pop('fields')
            self._payload_list.append(actual_input_combination)
    
    
    def synch_rest_call(self):
        rest_success = True
        error_message = None
        
        try:
            if len(self._payload_list) > 0:
                self._rest_response = self.wml_client.deployments.score(self.deployment_guid,meta_props={'input_data':self._payload_list})
        except ApiRequestFailure as err:
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
            rest_success = False
            error_message = str(err.args[0])
        except:
            tracer.error("Unknown exception: %s", str(sys.exc_info()[0]))
            logger.error("WMLOnlineScoring: Unknown exception: %s", str(sys.exc_info()[0]))
            #because the predictioon for whole bundle failed, the complete local_list is invalid
            rest_success = False
            error_message = str(sys.exc_info()[0])
            
        if rest_success:
            for item in self._status_list:
                if item["mapping_success"]:
                    item["score_success"] = True
        else:
            self._rest_response = None
            for item in self._status_list:
                item["score_success"] = False
                if item["message"] is None:
                    item["message"] = "WML API error: " + error_message

        tracer.debug("WMLOnlineScoring: Worker %d got %d predictions from WML model deployment!", self._handler_index, len(self._rest_response['predictions'][0]['values']))
    
        return len(self._rest_response['predictions']) # number of prediction response bundles
        

    def postprocess(self):

        #scoring REST call had error, no result to process
        #just the error fields have to be provided
        if self._rest_response is None:
            for index, item in enumerate(self._status_list):
                self._result_list[index] = {"PredictionError": item["message"]}
            return 
            
        #take the tuples from local list in sequence, sequence is same as the 
        #sequence of prediction 'values' as input was generated in sequence of the _data_list
        #there is no reference from input to prediction except the position in sequence
        #use output mapping function or just add the raw result to tuple
        #for later separation and processing
        #each prediction contains model result 'fields' and one or more 'values' lists
        #one value list for each scoring set
        # only data with successful mapping was added in payload and gets response data
        for prediction in self._rest_response['predictions']:
            response_index = 0 
            for data_index,item in enumerate(self._status_list):
                if item["mapping_success"] :
                    self._result_list[data_index] = {"Prediction" : dict(zip(prediction['fields'],prediction['values'][response_index]))}
                    response_index += 1 
                else:
                    self._result_list[data_index] = {"PredictionError": item["message"]}
                    

