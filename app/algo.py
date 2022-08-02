import numpy as np
import pandas as pd
import os
import sdv
from sdv.lite import TabularPreset
from sdv.evaluation import evaluate
import jsonpickle


class Client:
    input_data = None
    number_of_samples = None
    local_sum = None
    local_mean = None
    global_mean = None

    #LR:added these members
    syn_config = None
    syn_data_file = None
    syn_target_file = None
    syn_data = None 
    syn_headers = None
    syn_feature_dict = None
    syn_features = None
    syn_number_of_samples = None
    syn_allowed_percentage = None
    syn_nan_percentage = None
    syn_validation_set = None
    syn_metadata = None
    syn_model = None
    syn_new_data = None
    syn_global_data = None


    def __init__(self):
        pass

    def read_input(self, input_path):
        try:
            print("DBF11 current wd: ",os.getcwd())
            #self.input_data = pd.read_csv(input_path, header=None)""
            print("DBF12 trying to find config.json, is path correct? -----------")
            self.syn_config = pd.read_json("/config.json") #config file(dictionary)
            print("DBF13 found json")
            self.syn_data_file = self.syn_config["Input File"][0]
            print("DBF14 opened data file")
            #print(syn_data_file)
            self.syn_target_file = self.syn_config["Output File"][0]
            self.syn_data = pd.read_csv(str(self.syn_data_file))
            self.syn_headers = self.syn_data.columns

            self.syn_feature_dict = self.syn_config["Features"]

            self.syn_features = [] #names of relevant headers
            print("DBF52 done loading------------------------------------------------------------------------")
        except FileNotFoundError:
            print(f'File {INPUT_PATH} could not be found.', flush=True)
            exit()
        except Exception as e:
            print(f'File could not be parsed: {e}', flush=True)
            exit()

    def compute_local_synthetization(self):
        print("DBF16 comp synthetization")

        for feature in self.syn_config["Features"].keys():
            self.syn_features.append(feature) # wanted headers

        self.syn_allowed_percentage = self.syn_config["Percentage"][0]
        self.syn_number_of_samples = int(self.syn_config["N_samples"][0])
        

        #Remove unspecified columns
        for header in self.syn_headers:
            if not header in self.syn_features:
                self.syn_data.pop(header)
                #print("Deleting column", header, "because of configuration.")

        #Remove columns with to many NaNs
        self.syn_headers = self.syn_data.columns #fixed list of columns we need
        for header in self.syn_headers:
            self.nan_percentage = self.syn_data[header].isna().sum() / len(self.syn_data[header].index)
            if self.nan_percentage > self.syn_allowed_percentage:
                self.syn_data.pop(header)
                self.syn_headers = self.syn_headers.drop(header)
                #print("Deleting column", header, "because of" ,"{:.2%}".format(self.nan_percentage), "NaNs.")


        #removing unwanted nans and interpolating others
        numericals = []
        for x in self.syn_features:
            if self.syn_feature_dict[x][0] == "numerical":
                numericals.append(x)

        for numerical in numericals:
            self.syn_data[numerical] = pd.to_numeric(self.syn_data[numerical], errors='coerce')

        self.syn_data.interpolate(method='pad')

        #create validation set
        self.syn_validation_set = self.syn_data.sample(frac=0.3)
        self.syn_validation_set.reset_index()


        #Generating Metadata
        self.syn_metadata = {}
        self.syn_metadata["fields"] = {}

        for x in self.syn_headers:
            self.syn_metadata["fields"][x] = {}
            self.syn_metadata["fields"][x]["type"] = self.syn_feature_dict[x][0]
            if self.syn_feature_dict[x][0] == "numerical":
                self.syn_metadata["fields"][x]["subtype"] = self.syn_feature_dict[x][1]
        self.syn_metadata["constraints"] = []


        #fit a model to the data and generating data
        #model = TabularPreset(name='FAST_ML', metadata=metadata)
        self.syn_model = TabularPreset(name='FAST_ML', metadata=self.syn_metadata)

        self.syn_model.fit(self.syn_data)

        self.syn_new_data = self.syn_model.sample(num_rows=self.syn_number_of_samples)

        self.syn_new_data = self.syn_new_data.round(decimals = 2)        

        #print("DBF101: ",self.syn_new_data)

        ###self.syn_new_data.to_csv(self.syn_target_file, index=False)

        
        #convert data frame to json
        dataframe_as_json = self.syn_new_data.to_json(orient="split")
        self.syn_new_data = dataframe_as_json
        ##self.syn_new_data = self.syn_new_data.to_csv(self.syn_target_file, index=False)

        #taking only one cell doesnt crash the program
        #self.syn_new_data = self.syn_new_data.iloc[0]
        #self.syn_new_data = self.syn_new_data[0]

        #print("DBF102: ",self.syn_new_data, flush=True) #printing the json dataframe seems to crash the console

        #print("The generated data is", "{:.2%}".format(evaluate(self.syn_new_data, self.syn_validation_set, metrics=['KSTest'])), "accurate to the original data. If this is unsatisfactory try using less or different features, more data or removing more NaNs.")

        print("DBF17 done synthetization")

    def aggregate_syn_global_data(self, local_syn_data):
        #print("DBF0423: aggregating syn data now...")
        #print("DBF0424: local_syn_data to aggregate: ", local_syn_data)
        return local_syn_data

    def set_syn_global_data(self, syn_global_data):
        print("DBF988: ")
        ##print("DBF989: before: ", syn_global_data)
        ##syn_global_data_json = jsonpickle.encode(syn_global_data)
        ##print("DBF990: datatypes before/after: ", type(syn_global_data),"/",type(syn_global_data_json))
        ##print("DBF991: results: ", syn_global_data_json) #LR: this crashes the app
        ##self.syn_global_data = syn_global_data_json
        self.syn_global_data = syn_global_data

    def compute_local_mean(self):
        print("DBF15 trying compute local mean")
        self.number_of_samples = self.input_data.shape[1]
        self.local_sum = self.input_data.to_numpy().sum()
        print(f'Local sum: {self.local_sum}', flush=True)
        self.local_mean = self.local_sum / self.number_of_samples
        print(f'Local mean: {self.local_mean}', flush=True)

    def set_global_mean(self, global_mean):
        self.global_mean = global_mean

    def write_results(self, output_path):
        
        df = pd.DataFrame(self.syn_global_data[0])
        df_str = df.to_string()
        print("DBF149 df to string: ",df_str)

        '''
        #f = open(output_path, "a")
        f = open("krekirolle7.txt","a")
        #print("dbf graue, len list: ", len(self.syn_global_data))
        #print("DBF150: ",self.syn_global_data)        
        #f.write(self.syn_global_data[0].to_string())
        f.write(df_str)
        f.close()
        '''
        
        #for x in self.syn_global_data:
        #    print("x")
        

        # pd.set_option('display.max_rows', None)
        # pd.set_option('display.max_columns', None)
        # pd.set_option('display.width', None)
        # pd.set_option('display.max_colwidth', None)

        #pd.set_option('display.expand_frame_repr', False)
        
        #df = pd.DataFrame(self.syn_global_data[0])
        #pd.set_option('display.max_colwidth', None)
        #print(df)

        ##print(self.syn_global_data)
        print("erdbeer katze")


class Coordinator(Client):

    def compute_global_mean(self, local_means):
        return np.sum(local_means) / len(local_means)
