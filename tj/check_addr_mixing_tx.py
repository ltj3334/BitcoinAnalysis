from collections import defaultdict
import sqlite3 
import pandas as pd
import numpy as np
from tqdm import tqdm
import bisect

class Check_addr_mixing_tx():
    def __init__(self, index_db_path, core_db_path, service_db_path, target_addr_list_file, target_mixing_tx_list_file, tqdm_off = False):
        self.conIndex = sqlite3.connect(f"./{index_db_path}")
        self.curIndex = self.conIndex.cursor()
        self.conCore = sqlite3.connect(f"./{core_db_path}")
        self.curCore = self.conCore.cursor()
        self.conService = sqlite3.connect(f"./{service_db_path}")
        self.curService = self.conService.cursor()
        self.MixingTxList = pd.read_csv(target_mixing_tx_list_file, header = None) 
        self. tqdm_off = tqdm_off
        self.indexed_address_list = []
        self.addr_tx_dict = defaultdict(list)

        indexed_address_list = self._get_indexed_address_using_origin_address(target_addr_list_file)
        self.addr_tx_dict = self._make_tx_out_in_mixing_tx(indexed_address_list)
        self._check_tx_in_mixing_tx(self.addr_tx_dict, self.MixingTxList)


    def _get_indexed_address_using_origin_address(self, target_addr_list_file):
        target_addr = pd.read_csv(f"./{target_addr_list_file}", header = 0)
        print(f"Using Origin Address Value, Target Address List Load SUCCESS")
        print(f"Count of Address : {len(target_addr)}")
        target_addr = target_addr.drop_duplicates(['Address'])
        print(f"After Drop Duplicate Address : {len(target_addr)}")

        # origin_addr = np.asarray(target_addr['Address'], dtype = str)
        # # row = int(len(origin_addr) / 1999)
        # shaped_origin_addr = origin_addr.reshape((1999, -1))

        origin_addr = target_addr['Address'].values.tolist()
        n = 1999
        slicing_origin_addr = [origin_addr[i * n:(i + 1) * n] for i in range((len(origin_addr) + n - 1) // n )] 
        
        for addr_list in tqdm(slicing_origin_addr, desc = "Origin Addr To Index Addr", disable=self.tqdm_off):
            sql = f"SELECT AddrID.id FROM AddrID WHERE AddrID.addr IN ({','.join(['?']*len(addr_list))})"
            self.curIndex.execute(sql, addr_list)
            indexed_addr = np.array(self.curIndex.fetchall()).T[0]
            if not indexed_addr.any():
                continue
            else:
                self.indexed_address_list.extend(list(indexed_addr))

        return self.indexed_address_list


    def _make_tx_out_in_mixing_tx(self, addr_list:list):
        
        addr_tx_dict = defaultdict(list)
        for addr in tqdm(addr_list, desc ="Getting TxOut Tx", disable=self.tqdm_off):
            sql = f"select tx from TxOut where addr = {addr};"
            self.curCore.execute(sql)
            tx_list = self.curCore.fetchall()
            if tx_list:
                origin_tx_list = [x[0] for x in tx_list]
                addr_tx_dict[addr] = origin_tx_list

        return addr_tx_dict

    def _check_tx_in_mixing_tx(self, addr_tx_dict:defaultdict, MixingTxList:pd.DataFrame):

        MixingTxList.columns = ['Tx']
        mixing_tx_list = MixingTxList['Tx'].values.tolist()
        mixing_tx_list.sort()

        for addr, tx_list in tqdm(addr_tx_dict.items(), desc = "check tx in mixing tx", disable=self.tqdm_off):  
            for tx in tx_list:
                # print(len(mixing_tx_list), tx)
                yap = bisect.bisect_left(mixing_tx_list, tx)
                try:
                    if mixing_tx_list[yap] == tx:
                        print(tx, mixing_tx_list[yap])
                except:
                    continue        
        return True


# target_file = '220529_illegal_analysis_result.csv'
target_addr_list_file_path = '220523_abuse_only_address.csv'
# target_addr_list_file_path = '220529_abuse_only_address_short.csv'
# target_addr_list_file_path = '220217_elliptic_addr.csv'
# target_addr_list_file_path = '220706_new_address.csv'
# target_addr_list_file_path = '220217_elliptic_addr_short.csv'

# target_addr_list_file_path = '220711_chainalysis_ransomware_address.csv'
# target_addr_list_file_path = '220711_chainalysis_Darknet_address.csv'
# target_addr_list_file_path = '220711_chainalysis_Darknet_address_short.csv'
# target_addr_list_file_path = '220712_MerchantChainalysis_address.csv'
# target_addr_list_file_path = '220714_upbit_address.csv'
target_mixing_tx_list_file = "220720_0_to_730000_block_mixing_address.csv"
core_db_path = "dbv3-core.db"
index_db_path ="dbv3-index.db"
service_db_path = "dbv3-service.db"
check = Check_addr_mixing_tx(index_db_path, core_db_path, service_db_path, target_addr_list_file_path, target_mixing_tx_list_file)
