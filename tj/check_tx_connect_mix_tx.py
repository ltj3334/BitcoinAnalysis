<<<<<<< HEAD
from collections import defaultdict
import os
from pickle import FALSE
import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
import bisect

class Check_tx_connect_mixing_tx():
    def __init__(self, index_db_path, core_db_path, service_db_path, tx_list_file_path, target_mixing_tx_list_file, tqdm_off = False):
        self.conIndex = sqlite3.connect(f"./{index_db_path}")
        self.curIndex = self.conIndex.cursor()
        self.conCore = sqlite3.connect(f"./{core_db_path}")
        self.curCore = self.conCore.cursor()
        self.conService = sqlite3.connect(f"./{service_db_path}")
        self.curService = self.conService.cursor()
        self.MixingTxList = pd.read_csv(target_mixing_tx_list_file, header = None) 
        self.tqdm_off = tqdm_off
        self.indexed_address_list = []
        self.addr_tx_dict = defaultdict(list)
        self.tx_dataframe = pd.read_csv(tx_list_file_path,header=None)
        self.tx_dataframe.columns =['id','txid']
        self.save_list = []

        indexed_address_list = self._find_txout_addr()
        self.addr_tx_dict = self._make_tx_out_in_mixing_tx(indexed_address_list)
        self.save_list = self._check_tx_in_mixing_tx(self.addr_tx_dict, self.MixingTxList)

        target_name = tx_list_file_path.split("_")
        
        if self.save_list:
            save_df = pd.DataFrame(self.save_list)
            save_df.columns = ['Addr','Tx']
            save_df.to_csv(f"{target_name[0]}_{target_name[1]}_{target_name[2]}_{target_name[3]}_mixing_result.csv", header = True, index= False)

    def _find_txout_addr(self):
        indexed_address_list = []
        txid_list = self.tx_dataframe['id'].to_numpy()
        
        
        n = 1999
        slicing_indexed_tx_list = [txid_list[i * n:(i + 1) * n] for i in range((len(txid_list) + n - 1) // n )] 
        for tx_list in tqdm(slicing_indexed_tx_list, desc = "Getting Address List", disable=self.tqdm_off):
            _tx_list = list(map(str, tx_list))
            sql = f"select addr from TxOut where tx IN ({','.join(['?']*len(_tx_list))})"
            self.curCore.execute(sql, _tx_list)
            addr_list = self.curCore.fetchall()
            if addr_list:
                indexed_address_list.extend([x[0] for x in addr_list])
        
        return indexed_address_list
    

    def _make_tx_out_in_mixing_tx(self, addr_list:list):
        addr_tx_dict = defaultdict(list)

        n = 1999
        slicing_indexed_addr_list = [addr_list[i * n:(i + 1) * n] for i in range((len(addr_list) + n - 1) // n )] 
        for addr_list in tqdm(slicing_indexed_addr_list, desc = "Getting Txout Tx List", disable=self.tqdm_off):
            _addr_list= list(map(str, addr_list))


            sql = f"SELECT addr, tx FROM TxOut WHERE addr IN ({','.join(['?']*len(_addr_list))})"
            query = self.curCore.execute(sql, _addr_list)
            cols = [column[0] for column in query.description]
            results= pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
            addr_tx_dict.update({k: g["tx"].tolist() for k,g in results.groupby("addr")})
    


            # sql = f"select addr, tx from TxOut where addr IN ({','.join(['?']*len(_addr_list))})"
            # self.curCore.execute(sql, _addr_list)
            # addr_tx_list = self.curCore.fetchall()
            # if addr_tx_list:
            #     for addr, tx in addr_tx_list:
            #         addr_tx_dict[addr].extend([tx])
            

        # for addr in tqdm(addr_list, desc ="Getting TxOut Tx", disable=self.tqdm_off):
        #     sql = f"select tx from TxOut where addr = {addr};"
        #     self.curCore.execute(sql)
        #     tx_list = self.curCore.fetchall()
        #     if tx_list:
        #         origin_tx_list = [x[0] for x in tx_list]
        #         addr_tx_dict[addr] = origin_tx_list

        return addr_tx_dict
    

    def _check_tx_in_mixing_tx(self, addr_tx_dict:defaultdict, MixingTxList:pd.DataFrame):

        MixingTxList.columns = ['Tx']
        mixing_tx_list = MixingTxList['Tx'].values.tolist()
        mixing_tx_list.sort()
        save_tx_list = []

        for addr, tx_list in tqdm(addr_tx_dict.items(), desc = "check tx in mixing tx", disable=self.tqdm_off):  
            for tx in tx_list:
                # print(len(mixing_tx_list), tx)
                yap = bisect.bisect_left(mixing_tx_list, tx)
                try:
                    if mixing_tx_list[yap] == tx:
                        save_tx_list.append([addr, tx])
                except:
                    continue        
        return save_tx_list



target_mixing_tx_list_file = "0_to_730000_block_mixing_address.csv"
core_db_path = "dbv3-core.db"
index_db_path ="dbv3-index.db"
service_db_path = "dbv3-service.db"
file = '600_to_700_dollar_result.csv'
if __name__ == "__main__":        
    check = Check_tx_connect_mixing_tx(index_db_path, core_db_path, service_db_path, file, target_mixing_tx_list_file, tqdm_off=False)
=======
from collections import defaultdict
import os
from pickle import FALSE
import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
import bisect

class Check_tx_connect_mixing_tx():
    def __init__(self, index_db_path, core_db_path, service_db_path, tx_list_file_path, target_mixing_tx_list_file, tqdm_off = False):
        self.conIndex = sqlite3.connect(f"./{index_db_path}")
        self.curIndex = self.conIndex.cursor()
        self.conCore = sqlite3.connect(f"./{core_db_path}")
        self.curCore = self.conCore.cursor()
        self.conService = sqlite3.connect(f"./{service_db_path}")
        self.curService = self.conService.cursor()
        self.MixingTxList = pd.read_csv(target_mixing_tx_list_file, header = None) 
        self.tqdm_off = tqdm_off
        self.indexed_address_list = []
        self.addr_tx_dict = defaultdict(list)
        self.tx_dataframe = pd.read_csv(tx_list_file_path,header=None)
        self.tx_dataframe.columns =['id','txid']
        self.save_list = []

        indexed_address_list = self._find_txout_addr()
        self.addr_tx_dict = self._make_tx_out_in_mixing_tx(indexed_address_list)
        self.save_list = self._check_tx_in_mixing_tx(self.addr_tx_dict, self.MixingTxList)

        target_name = tx_list_file_path.split("_")
        
        if self.save_list:
            save_df = pd.DataFrame(self.save_list)
            save_df.columns = ['Addr','Tx']
            save_df.to_csv(f"{target_name[0]}_{target_name[1]}_{target_name[2]}_{target_name[3]}_mixing_result.csv", header = True, index= False)

    def _find_txout_addr(self):
        indexed_address_list = []
        txid_list = self.tx_dataframe['id'].to_numpy()
        
        for tx in txid_list:
            sql = f"select addr from TxOut where tx = {str(tx)};"
            self.curCore.execute(sql)
            addr_list = self.curCore.fetchall()
            if addr_list:
                indexed_address_list.extend([x[0] for x in addr_list])
            
        
        return indexed_address_list
    

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
        save_tx_list = []

        for addr, tx_list in tqdm(addr_tx_dict.items(), desc = "check tx in mixing tx", disable=self.tqdm_off):  
            for tx in tx_list:
                # print(len(mixing_tx_list), tx)
                yap = bisect.bisect_left(mixing_tx_list, tx)
                try:
                    if mixing_tx_list[yap] == tx:
                        save_tx_list.append([addr, tx])
                except:
                    continue        
        return save_tx_list



target_mixing_tx_list_file = "0_to_730000_block_mixing_address.csv"
core_db_path = "dbv3-core.db"
index_db_path ="dbv3-index.db"
service_db_path = "dbv3-service.db"
file = '600_to_700_dollar_result.csv'
if __name__ == "__main__":        
    check = Check_tx_connect_mixing_tx(index_db_path, core_db_path, service_db_path, file, target_mixing_tx_list_file, tqdm_off=False)
>>>>>>> 7bd933d3d4ba6c0da75a583deb3f14c482974b95
