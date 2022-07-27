import pandas as pd
import numpy as np
import sqlite3
import bisect
from collections import defaultdict
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
import seaborn as sns
import matplotlib.pyplot as plt 
import math



class illegal_analysis_using_cosine_similarity():
    def __init__(self, target_file, index_db_path, core_db_path, service_db_path):
        self.conIndex = sqlite3.connect(f"./{index_db_path}")
        self.curIndex = self.conIndex.cursor()
        self.conCore = sqlite3.connect(f"./{core_db_path}")
        self.curCore = self.conCore.cursor()
        self.conService = sqlite3.connect(f"./{service_db_path}")
        self.curService = self.conService.cursor()

        raw_illegal_df = pd.read_pickle(target_file)
        
        raw_illegal_df = raw_illegal_df.drop_duplicates(['AddressID'])
        raw_illegal_df = pd.concat([raw_illegal_df['AddressID'], raw_illegal_df['MI_BTCIn_M1'], raw_illegal_df['MI_BTCIn_M2'], raw_illegal_df['MI_BTCIn_M3'], raw_illegal_df['MI_BTCIn_M4']], axis=1)

        addr_df = raw_illegal_df['AddressID']
        addr_list = addr_df.values.tolist()
        addr_list = self._get_addr_in_cluster(addr_list)
        target_dataframe = self._get_cluster_id_contents(raw_illegal_df, addr_list)
        cosine_similarity_result = self._calc_cosine_similarity(target_dataframe)
        self._print_result(cosine_similarity_result)


        sns.heatmap(cosine_similarity_result)
        plt.show()

    def _get_addr_in_cluster(self, address_list:list):
        addr_cluster_hash = defaultdict(list)
        cluster_result_array = []

        for addr in address_list:
            sql=f"SELECT cluster FROM Cluster WHERE addr = ({addr})"
            self.curService.execute(sql)
            cluster_result_array.extend(np.array(self.curService.fetchone()))
                
        for index_addr, cluster_id in zip(address_list, cluster_result_array):
            if len(addr_cluster_hash[cluster_id]) == 0:
                addr_cluster_hash[cluster_id].append(index_addr)
        result_list = list(np.array(list(addr_cluster_hash.values())).T[0])

        return result_list 

    def _get_cluster_id_contents(self, raw_dataframe:pd.DataFrame, cluster_id_list):
        raw_dataframe = raw_dataframe.sort_values('AddressID')
        cluster_id_list.sort()

        address_id_list = list(raw_dataframe['AddressID'])

        result_list = []
        
        for id in cluster_id_list:
            _index = bisect.bisect(address_id_list, id)
            result_list.append(list(raw_dataframe.iloc[_index].values)) # AddressID가 플롯임

        new_dataframe = pd.DataFrame(result_list)
        new_dataframe.columns = ['AddressID', 'MI_BTCIn_M1', 'MI_BTCIn_M2','MI_BTCIn_M3','MI_BTCIn_M4']
        new_dataframe = new_dataframe.astype({'AddressID':'int'})

        return new_dataframe
        
    def _calc_cosine_similarity(self, target_dataframe:pd.DataFrame):

        target_dataframe = pd.concat([target_dataframe['MI_BTCIn_M1'], target_dataframe['MI_BTCIn_M2'], target_dataframe['MI_BTCIn_M3'], target_dataframe['MI_BTCIn_M4']], axis=1)
        normalized_df=(target_dataframe-target_dataframe.min())/(target_dataframe.max()-target_dataframe.min())
        cos_sim_array = cosine_similarity(normalized_df)

        return cos_sim_array

    def _print_result(self, result_list:np.array, accuracy = 0.9):
        
        new_cos_sim_array = np.where(result_list < accuracy, 0, result_list)
        new_cos_sim_array = np.where(result_list >= accuracy, 1, result_list)

        map_count = len(new_cos_sim_array)
        result = np.sum(new_cos_sim_array)

        total_result = (math.sqrt(result)) / map_count * 100

        print(f"Address's Count {map_count}")
        print(f"Over {accuracy} similarity Percentage : {total_result}")

        return 0



if __name__ == "__main__":
    target_file = '220217_elliptic_addr_2022-02-18.pkl'
    core_db_path = "dbv3-core.db"
    index_db_path ="dbv3-index.db"
    service_db_path = "dbv3-service.db"
    il = illegal_analysis_using_cosine_similarity(target_file, index_db_path, core_db_path, service_db_path)