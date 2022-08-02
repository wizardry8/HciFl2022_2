import jsonpickle
import pandas as pd
import threading
import time
import yaml
import shutil
import sdv
from sdv.lite import TabularPreset
from sdv.evaluation import evaluate

from app.algo import Coordinator, Client





class AppLogic:

    def __init__(self):
        # === Status of this app instance ===

        # Indicates whether there is data to share, if True make sure self.data_out is available
        self.status_available = False

        # Only relevant for coordinator, will stop execution when True
        self.status_finished = False

        # === Parameters set during setup ===
        self.id = None
        self.coordinator = None
        self.clients = None

        # === Data ===
        self.data_incoming = []
        self.data_outgoing = None

        # === Internals ===
        self.thread = None
        self.iteration = 0
        self.progress = 'not started yet'

        # === Custom ===
        self.INPUT_DIR = "/mnt/input"
        self.OUTPUT_DIR = "/mnt/output"

        self.data_name = None

        self.client = None

    def handle_setup(self, client_id, coordinator, clients):
        # This method is called once upon startup and contains information about the execution context of this instance
        self.id = client_id
        self.coordinator = coordinator
        self.clients = clients
        print(f'Received setup: {self.id} {self.coordinator} {self.clients}', flush=True)

        self.thread = threading.Thread(target=self.app_flow)
        self.thread.start()

    def handle_incoming(self, data):
        # This method is called when new data arrives
        print("Process incoming data....", flush=True)
        self.data_incoming.append(data.read())

    def handle_outgoing(self):
        print("Process outgoing data...", flush=True)
        # This method is called when data is requested
        self.status_available = False
        return self.data_outgoing

    def read_config(self):
        #with open(self.INPUT_DIR + '/config.yml') as f:
        #    config = yaml.load(f, Loader=yaml.FullLoader)['fc_mean']
        #    self.input_name = config['input_name']
        #    self.output_name = config['output_name']

        self.input_name = 'data.csv'
        self.output_name = 'output.csv'

        #shutil.copyfile(self.INPUT_DIR + "/config.yml", self.OUTPUT_DIR + "/config.yml")

    def app_flow(self):
        # This method contains a state machine for the client and coordinator instance

        # === States ===
        state_initializing = 1
        state_read_input = 2
        state_local_computation = 6
        state_wait_for_aggregation = 7
        state_global_aggregation = 8
        state_writing_results = 9
        state_finishing = 10

        # Initial state
        state = state_initializing
        self.progress = 'initializing...'

        while True:
            if state == state_initializing:
                print("Initializing", flush=True)
                if self.id is not None:  # Test if setup has happened already
                    print(f'Coordinator: {self.coordinator}', flush=True)
                    if self.coordinator:
                        self.client = Coordinator()
                    else:
                        self.client = Client()
                    state = state_read_input

            if state == state_read_input:
                print("Read input", flush=True)
                self.progress = 'read input'
                self.read_config()
                self.client.read_input(self.INPUT_DIR + '/' + self.input_name)
                state = state_local_computation                

            if state == state_local_computation:
                print("Local mean computation", flush=True)
                self.progress = 'local computation'
                self.client.compute_local_synthetization()

                #self.syn_new_data.to_csv(self.syn_target_file, index=False) ##LR: this should be the synthetisized final data from algo

                #print("DBF005: type of self.lient.syn_new_data: ", type(self.client.syn_new_data))
                #syn_data_to_send = jsonpickle.encode(self.client.syn_new_data) ##LR: try sending without pickleing data
                #print("DBF006: type of self.lient.syn_new_data after jsonpickle: ", type(syn_data_to_send))
                
                #syn_data_to_send = self.client.syn_new_data #LR: before changing to pickle again
                syn_data_to_send = jsonpickle.encode(self.client.syn_new_data) ##LR: try sending without pickleing data
                ##unpickled_syn_data_to_send = jsonpickle.decode(syn_data_to_send)
                ##print("DBF000001: unpickled: ",unpickled_syn_data_to_send)

                if self.coordinator:
                    #print("DBF007: unjsonpickled data: ", self.client.syn_new_data)
                    #print("DBF008: just before crash")
                    #print("DBF009: appending syn_data_to_send: ", syn_data_to_send)
                    self.data_incoming.append(syn_data_to_send)
                    ##self.data_incoming.append(self.client.syn_new_data)
                    state = state_global_aggregation
                else:
                    self.data_outgoing = syn_data_to_send
                    self.status_available = True
                    state = state_wait_for_aggregation
                    print(f'[CLIENT] Sending local mean data to coordinator', flush=True)


                # if self.coordinator:
                #     self.data_incoming.append(data_to_send)
                #     state = state_global_aggregation
                # else:
                #     self.data_outgoing = data_to_send
                #     self.status_available = True
                #     state = state_wait_for_aggregation
                #     print(f'[CLIENT] Sending local mean data to coordinator', flush=True)

            if state == state_wait_for_aggregation:
                print("Wait for aggregation", flush=True)
                self.progress = 'wait for aggregation'
                if len(self.data_incoming) > 0: ##LR: TODO: adapt this to syn
                    print("Received global mean from coordinator.", flush=True)
                    global_mean = jsonpickle.decode(self.data_incoming[0])
                    self.data_incoming = []
                    self.client.set_global_mean(global_mean)
                    state = state_writing_results

            # GLOBAL PART
            if state == state_global_aggregation:
                print("Global computation", flush=True)
                self.progress = 'global aggregation...'
                
                if len(self.data_incoming) == len(self.clients):

                    #print("DBF 590: self.data_incoming: ",self.data_incoming[0])
                    
                    #local_means = [jsonpickle.decode(client_data) for client_data in self.data_incoming] ##LR: this crashes because data is not pickled
                    #local_syn_data_for_global_part = [data_for_global for data_for_global in self.data_incoming] ##LR: before changing to pickle again
                    print("DBF244: datatype before decode: ", type(self.data_incoming))
                    print("J")
                    ##local_syn_data_for_global_part_temp = [jsonpickle.decode(data_for_global) for data_for_global in self.data_incoming]
                    local_syn_data_for_global_part = [pd.read_json(jsonpickle.decode(data_for_global),orient="split") for data_for_global in self.data_incoming]
                    #print("DBF590,5: local_syn_data_for_global_part datatype/length: ", type(local_syn_data_for_global_part),"/",len(local_syn_data_for_global_part))
                    print("K") 
                    
                    #pd.read_json(exp_data_unpickled,orient="split") #LR: just reference for pd.read_json, delete later
                    #print("len temp: ", len(local_syn_data_for_global_part_temp))
                    #local_syn_data_for_global_part = pd.read_json(local_syn_data_for_global_part_temp[0],orient="split")              
                    self.data_incoming = []
                    #global_mean = self.client.compute_global_mean(local_means) #LR: original code
                    #print("DBF 591: local_syn_data_for_global_part (aka local syn data: ", local_syn_data_for_global_part)
                    syn_global_data = self.client.aggregate_syn_global_data(local_syn_data_for_global_part) #LR: this is dummy method for now
                    #print("DBF 592: global_syn_data: ", syn_global_data)
                    print("P")

                    self.client.set_syn_global_data(syn_global_data)
                    print("I")
                    # data_to_broadcast = jsonpickle.encode(global_mean)
                    data_to_broadcast = syn_global_data #LR: not jsonpickle because pandasdataframe, if not working this way can convert to string by native pandas method and pickle
                    print("G")
                    # self.data_outgoing = data_to_broadcast
                    self.data_outgoing = data_to_broadcast

                    print("O")
                    self.status_available = True
                    print("data out: ", self.data_outgoing)
                    print("R")
                    state = state_writing_results
                    print(f'[COORDINATOR] Broadcasting global mean to clients', flush=True)

            if state == state_writing_results:
                print("Writing results", flush=True)
                # now you can save it to a file                
                self.client.write_results(self.OUTPUT_DIR + '/' + self.output_name)
                state = state_finishing

            if state == state_finishing:
                print("Finishing", flush=True)
                self.progress = 'finishing...'
                if self.coordinator:
                    time.sleep(10)
                #self.status_finished = True  ##LR: IMPORTANT REMOVE THIS BEFORE RUNNING FINAL TESTS THIS IS JUST TO PREVENT APP FROM CLOSING
                #break

            time.sleep(1)


logic = AppLogic()