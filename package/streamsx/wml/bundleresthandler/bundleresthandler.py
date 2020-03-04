# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
   
'''

class ProcessStorage stores 
    - a set of elements of the input data
    - all status and result information related to each of single input element


- it is needed for copy and hold a slice of input data 
- map mining fields (parameter mapping_dict)
    - mapping status may be: successful | invalid_data
    - mapping status has to be stored for each element
- mapping generates scoring request payload for each element
- payload may be devided by 'field' combinations if optional fields are allowed (backlog feature)
- payload dividing depends ordered and unordered submission behavior

'''   

import time
import sys   
import logging



   
tracer = logging.getLogger(__name__)   
release = False
if not release:
    tracer.setLevel(logging.DEBUG)
    tracer.addHandler(logging.StreamHandler(sys.stdout))
   
   
   
   
   
class BundleRestHandler():

    max_copy_size = 100 
    '''max input items to be copied from input queue, to be set by using application, defaults to 100'''
    source_data_list = None
    '''Reference to the source where data bundles should be read from.'''
    input_list_lock = None
    '''lock for the input queue, this lock is used by different threads acessing the queuue, to be set by using application'''
    field_mapping = None
    '''list with input data attribute to mining model field mapping'''
    keep_data_order = True
    '''For future use: The actual version doesn't support unordered output
    Defines if output data shall be send in same order as input data. 
    This has only impact if optional fields are allowed.
    '''
    single_output = True
    ''' Defines if all results should be written to one output or two (success,error) '''
    allow_optional_fields = False
    ''' For future use: The actual version doesn't support optional fields.
    '''
    bundle_counter = 0
    ''' Counter incremented by each copy of a bundle from source. Reference to an outside variable.
    '''
    next_bundle_to_sent = 0
    ''' Number (derived from bundle_counter) of next bundle to be send. Needed in case keep_data_order = True
    Reference to an outside variable.
    '''
    output_function = None
    ''' Reference of the output function to be used. Depending on single_output setting it has to support
    one or two parameters of type list'''
    
    def __init__(self, handler_index):
        '''ProcessStorage is created build up by copying oldest input data from input queue
        and deleting them from this queue.
        '''
        tracer.debug("Enter init")
        assert self.input_list_lock is not None
        assert self.source_data_list is not None
        assert self.field_mapping is not None
        assert self.output_function is not None
        #if keep_data_order:
        #    assert self.bundle_counter is not None
        #    assert self.next_bundle_to_send is not None
        
        #####################################################################
        # thread related members
        #
        #####################################################################
        self._handler_index = handler_index
        self._run = True
        
        #####################################################################
        # Data processing members
        #
        #####################################################################
        self._data_size = 0
        self._bundle_number = None
        self._data_list = []            # don't change this, this is the original data, filled by copy()
        self._status_list = []          # processing status of several steps, if one step is not successful, next are not done
                                        # will get same size as _data_list
        self._payload_list = []         # REST payload list, each element is one payload, generated by preprocess()
        self._rest_response = []        # holds the REST response, REST errors are reflected in _status_list
        self._result_list = []          # REST result list, needs to have one entry for each data being in payload, result index equals to data index 


    def run(self):
        tracer.debug("Starting handler: %d with threads run method", self._handler_index  )
        
        bundle_tuple_count = 0
        overall_count = 0
        #as long as thread shall not stop
        while self._run:
            bundle_tuple_count = self.copy_from_source()
            tracer.debug("Loop: Thread %d received %d tuples.", self._handler_index, bundle_tuple_count )
            if  bundle_tuple_count > 0:
                self.preprocess()
                self.synch_rest_call()
                self.postprocess()
                self.write_result_to_output()
                overall_count += bundle_tuple_count
                
        tracer.info("Handler %d stopped after %d records", self._handler_index, overall_count)

            
    def stop(self):
        tracer.debug("Stopping handler: %d", self._handler_index  )
        self._run = False


    def copy_from_source(self):        
        self._data_size = 0
        self._bundle_number = None
        self._data_list = []            # don't change this, this is the original data, filled by copy()
        self._status_list = []          # processing status of several steps, if one step is not successful, next are not done
                                        # will get same size as _data_list
        self._payload_list = []         # REST payload list, each element is one payload, generated by preprocess()
        self._rest_response = []        # holds the REST response, REST errors are reflected in _status_list
        self._result_list = []          # REST result list, needs to have one entry for each data being in payload, result index equals to data index 

        # python threading is just sequential processing, staying little longer in lock doesn't matter
        with self.input_list_lock:
            
            # wait blocking with timeout (to allow checking outer run condition) 
            # until list has been filled
            if not self.input_list_lock.wait_for(lambda:len(self.source_data_list) > 0 , 0.1): 
                return 0
        
            #determine size and copy max size or all to local data list
            input_size = len(self.source_data_list)
            #tracer.debug("ProcessStorage (%d) : source_data_list len before copy %d!", self._handler_index, input_size)
            if input_size > 0:
                end_index = int(self.max_copy_size) if input_size >= self.max_copy_size else input_size
                self._data_list = self.source_data_list[:end_index]
                del self.source_data_list[:end_index]
                self._data_size = end_index 
                #tracer.debug("ProcessStorage (%d) :  read %d tuples from input queue with _data_list len %d!", self._handler_index, end_index, len(self._data_list))
                self._bundle_number = self.bundle_counter
                self.bundle_counter += 1
    
                #create the status list at once
                self._status_list = [{"mapping_success":False,"score_success":False,"message":None} for i in range(self._data_size)]
                #create the result list at once
                self._result_list = [None for i in range(self._data_size)]
                tracer.debug("ProcessStorage (%d) : source_data_list len after copy %d!", self._handler_index, len(self.source_data_list))
                
                #wake up waiting threads inclusive writing threads
                self.input_list_lock.notify()
        return self._data_size                
        
    def get_final_data(self, single_list = True):
        if single_list:
            single_output = [{**data,**result} for data, result in zip(self._data_list, self._result_list)]
            return [single_output]
        else:
            success_output = [{**data,**result} for data, result, status in zip(self._data_list, self._result_list, self._status_list) if status["message"] is None]
            error_output = [{**data,**result} for data, result, status in zip(self._data_list, self._result_list, self._status_list) if status["message"] is not None]
            return [success_output, error_output]
    
    def write_result_to_output(self):
        while self.next_bundle_to_sent != self._bundle_number:
            time.sleep(0.01)
        self.output_function(self.get_final_data(single_list = self.single_output))
        self.next_bundle_to_sent = self.next_bundle_to_sent + 1
    
    def get_bundle_number(self):
        return self._bundle_number
    
    def get_status(self):
        return self._status_list

    def get_payload(self):
        return self._payload_list
      
    def get_rest_response(self):
        return self._rest_response

    def get_postprocess_result(self):
        return self._result_list


    #######################################################
    # to be defined by derived class
    #
    # they are specific for different asynch calls
    #######################################################        
    def preprocess(self):
        raise NotImplementedError

    def postprocess(self):
        raise NotImplementedError

    def synch_rest_call(self):
        raise NotImplementedError

