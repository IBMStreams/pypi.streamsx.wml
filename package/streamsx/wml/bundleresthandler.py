# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
   
'''

class ProcessStorage stores 
    - a set of elements of the input data
    - all status and result information related to each of single i nput element


- it is needed for copy and hold a slice of input data 
- map mining fields (parameter mapping_dict)
    - mapping status may be: successful | invalid_data
    - mapping status has to be stored for each element
- mapping generates scoring request payload for each element
- payload may be devided by 'field' combinations if optional fields are allowed (backlog feature)
- payload dividing depends ordered and unordered submission behavior

'''   
   
import logging
   
tracer = logging.getLogger(__name__)   
   
   
   
_STREAMSX_MAPPING_ERROR_MISSING_MANDATORY = "Missing mandatory input field: "
   
class BundleRestHandler():

    max_copy_size = 100 
    '''max input items to be copied from input queue, to be set by using application, defaults to 100'''
    input_list_lock = None
    '''lock for the input queue, this lock is used by different threads acessing the queuue, to be set by using application'''
    field_mapping = None
    '''list with input data attribute to mining model field mapping'''
    keep_data_order = True
    '''Defines if output data shall be send in same order as input data. 
    This has only impact if optional fields are allowed.
    '''
    allow_optional_fields = False
    ''' The actual version doesn't support optional fields, even if they are given in mapping
    '''

    def __init__(self, storage_id, input_queue):
        '''ProcessStorage is created build up by copying oldest input data from input queue
        and deleting them from this queue.
        '''
        tracer.debug(__name__,"Enter init")
        assert self.input_list_lock is not None
        
        self._storage_id = storage_id
        self._data_size = 0
        self._data_list = []            # don't change this, this is the original data, filled by copy()
        self._status_list = []          # processing status of several steps, if one step is not successful, next are not done
                                        # will get same size as _data_list
        self._payload_list = []         # REST payload list, each element is one payload, generated by preprocess()
        self._rest_response = []        # holds the REST response, REST errors are reflected in _status_list
        self._result_list = []          # REST result list, needs to have one entry for each data being in payload, result index equals to data index 
        
        with self.input_list_lock:
            #determine size and copy max size or all to local list
            input_size = len(input_queue)
            tracer.debug("ProcessStorage (%d) : input_queue len before copy %d!", self._storage_id, input_size)
            if input_size > 0:
                end_index = int(self.max_copy_size) if input_size >= self.max_copy_size else input_size
                self._data_list = input_queue[:end_index]
                del input_queue[:end_index]
                self._data_size = end_index 
                tracer.debug("ProcessStorage (%d) :  read %d tuples from input queue with _data_list len %d!", self._storage_id, end_index, len(self._data_list))
            tracer.debug("ProcessStorage (%d) : input_queue len after copy %d!", self._storage_id, len(input_queue))
    
        #create the status list at once
        self._status_list = [{"mapping_success":False,"score_success":False,"message":None} for i in range(self._data_size)]
        #create the result list at once
        self._result_list = [None for i in range(self._data_size)]
        


    def preprocess(self):
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
        # this is a sample where all fields are required and are anytime in the input tuple
        # model fields have to be in order/sequence as expected by the model

        assert self.field_mapping is not None
    
        # keep this assert as long as we don't support optional fields
        assert self.allow_optional_fields is False
        
        # empty bundle list
        self._payload_list = []
        
        actual_input_combination ={'fields':[],'values':[]}
        for index,_tuple in enumerate(self._data_list):
            tuple_values = []
            tuple_fields = []
            tuple_is_valid = True
            for field in self.field_mapping:
                if field['tuple_field'] in _tuple and _tuple[field['tuple_field']] is not None:
                    tuple_values.append(_tuple[field['tuple_field']])
                    tuple_fields.append(field['model_field'])
                elif self.allow_optional_fields:
                    if field['is_mandatory']:
                        tuple_is_valid = False
                        break
                else:
                    tuple_is_valid = False
                    break

            if tuple_is_valid: 
                self._status_list[index]["mapping_success"] = True
            else:
                self._status_list[index]["mapping_success"] = False
                self._status_list[index]["message"] = _STREAMSX_MAPPING_ERROR_MISSING_MANDATORY + field['tuple_field']
                continue            
                
            if actual_input_combination['fields'] == tuple_fields:
                #same fields as before, just add further values
                actual_input_combination['values'].append(list(tuple_values))
            else:
                #close and store last fields/values combination in final _payload_list
                #except for the first valid tuple being added
                if len(actual_input_combination['values']) > 0 : self._payload_list.append(actual_input_combination) 
                #create new field/value combination
                actual_input_combination['fields']=tuple_fields
                actual_input_combination['values']=[list(tuple_values)]
                        
        #after last tuple store the open field/value combination finally in bundl_list
        self._payload_list.append(actual_input_combination)

    def preprocess(self):
        return 

    def synch_rest_call(self):
        return    

    
    def get_status(self):
        return self._status_list

    def get_payload(self):
        return self._payload_list
      
    def get_rest_response(self):
        return self._rest_response

    def get_result_data(self):
        return self._result_list
        
    def get_final_data(self):
        return         