
import csv
import pandas as pd
import numpy as np
import sqlite3
from collections import defaultdict
from tqdm import tqdm
from collections import deque

class find_mixer():
    def __init__(self, index_db_path, core_db_path, service_db_path, block_range:list, tqdm_off = False):
        self.conIndex = sqlite3.connect(f"./{index_db_path}")
        self.curIndex = self.conIndex.cursor()
        self.conCore = sqlite3.connect(f"./{core_db_path}")
        self.curCore = self.conCore.cursor()
        self.conService = sqlite3.connect(f"./{service_db_path}")
        self.curService = self.conService.cursor()
        self.addr_list = []
        self.tx_list = []
        self.tx_chain_list = defaultdict(list)
        self.full_chain_list = []
        self.tqdm_off = tqdm_off
        
        self.save_transaction = []
        


        target_tx_list = self._get_tx_using_block_range(block_range)
        
        
        # Algorithm 3   Finding suspected transactions of a block as a candidate mixing transaction
        self.tx_list = self._get_transaction_one_input_two_output_addr_p2sh(target_tx_list)
        self.tx_list = self._check_input_output_value(self.tx_list)
        # Algorithm 1
        self.full_chain_list = self._make_full_chain_mixing_transaction(self.tx_list)
        

        for tx, tx_list in self.full_chain_list.items():
            if tx_list[0] not in self.save_transaction:
                self.save_transaction.append(tx_list[0])
        
        # save_item = pd.DataFrame(save_transaction)
        # save_item.columns = ['Mixing Transaction Number']

        # save_item.to_csv('220717_mixing_transaction_list.csv', index = False)


    def _get_tx_using_block_range(self, block_range:list):
        # print(f"Using BlockHeight to get transaction")
        latest_block = block_range[1]
        oldest_block = block_range[0]
        target_block_list = []
        for i in range(oldest_block, latest_block+1):
            target_block_list.append(i)       
        

        # print(f"Target Block COunt : {len(target_block_list)}")

        n = 1999
        slicing_block_range = [target_block_list[i * n:(i + 1) * n] for i in range((len(target_block_list) + n - 1) // n )] 
        target_tx_list = []
        for block_list in tqdm(slicing_block_range, desc = "Origin Addr To Index Addr", disable= self.tqdm_off):
            sql = f"SELECT tx FROM BlkTx WHERE blk IN ({','.join(['?']*len(block_list))})"
            self.curCore.execute(sql, block_list)
            tx_list = np.array(self.curCore.fetchall()).T[0]
            if not tx_list.any():
                continue
            else:
                target_tx_list.extend(list(tx_list))

        return target_tx_list

    def _get_transaction_one_input_two_output_addr_p2sh(self, tx_list:list):
        
        # filtered_tx_list = defaultdict(list)
        filtered_tx_list = []

        for tx in tqdm(tx_list, desc = "filtering one input two output",disable= self.tqdm_off):
            self.curCore.execute(f"SELECT count(tx) from TxOut where tx =  ({str(tx)})")
            temp_list = self.curCore.fetchall()

            if temp_list[0][0] == 2:
                self.curCore.execute(f"SELECT count(tx) from TxIn where tx =  ({str(tx)})")
                temp_list = self.curCore.fetchall()
                    
                if temp_list[0][0] == 1:
                    if self.__check_addr_is_p2sh(tx) == True:
                        filtered_tx_list.append(tx)

                else: 
                    continue
            else:
                continue
        
        return filtered_tx_list
        
    def _check_transaction_one_input_two_output_addr_p2sh(self, tx):
        

        self.curCore.execute(f"SELECT count(tx) from TxOut where tx =  ({str(tx)})")
        temp_list = self.curCore.fetchall()

        if temp_list[0][0] == 2:
            self.curCore.execute(f"SELECT count(tx) from TxIn where tx =  ({str(tx)})")
            temp_list = self.curCore.fetchall()
                
            if temp_list[0][0] == 1:
                if self.__check_addr_is_p2sh(tx) == True:
                    return True

                else:
                    return False
            else:
                return False
        else:
            return False

    def __check_addr_is_p2sh(self, tx):
        self.curCore.execute(f"SELECT ptx, pn from TxIn where tx =  ({str(tx)})")
        tx, n = self.curCore.fetchone()
        self.curCore.execute(f"SELECT addr FROM TxOut where tx = {str(tx)} and n = {str(n)}")
        if tx == 0 and n == 0:
            return False
        else:
            addr = self.curCore.fetchone()[0]
        
        self.curIndex.execute(f"SELECT addr FROM AddrID where id = {str(addr)}")
        origin_addr = self.curIndex.fetchone()[0]

        if origin_addr[0] == '3':
            self.curCore.execute(f"SELECT addr FROM TxOut where tx = {str(tx)}")
            return_list = self.curCore.fetchall()
            return_list = [x[0] for x in return_list]

            for i_addr in return_list:
                self.curIndex.execute(f"SELECT addr FROM AddrID where id = {str(i_addr)}")
                origin_addr = self.curIndex.fetchone()[0]

                if origin_addr[0] == '3':
                    return True
                
            return False

        else:
            return False                 

    def _check_input_output_value(self, tx_list:(list)):

        filtered_tx_list = []


        for tx in tqdm(tx_list, desc = "check input output value", disable=self.tqdm_off):
            self.curCore.execute(f"SELECT btc from TxOut where tx = ({tx})")
            btc_list = self.curCore.fetchall()
            btc_list = [x[0] for x in btc_list]
            #output check more 5 time
            max_btc, min_btc = max(btc_list), min(btc_list)
            if max_btc > min_btc * 5:
                self.curCore.execute(f"SELECT Txout.btc FROM TxIn INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n WHERE TxIn.tx = {tx}")
                input_value = self.curCore.fetchone()
                               
                if not input_value:
                    continue
                elif input_value[0] >= 0.9:
                    filtered_tx_list.extend([tx])
                else:
                    continue
            else:
                continue
                
        return filtered_tx_list
                
    def _make_full_chain_mixing_transaction(self, tx_list:list):


        tx_chain_list = defaultdict(list)

        for tx in tqdm(tx_list, desc = "Make Full Chain of Mixing Transaction : Backward Part", disable=self.tqdm_off):
            
            origin_tx = tx

            temp_tx = tx
            temp_ptx_list = self.__return_ptx_list(tx)
            ptx_list = []
            temp_chain_list = []
            found_flag = False
            while len(temp_chain_list) < 10 and found_flag is False:
                if self.__is_sweeper(temp_tx):
                    temp_chain_list.extend([temp_tx])
                    if not tx_chain_list[tx]:
                        temp_chain_list.reverse()
                        tx_chain_list[origin_tx].extend(temp_chain_list)
                    found_flag = True
                    break
                
                elif self._check_transaction_one_input_two_output_addr_p2sh(temp_tx):
                    temp_chain_list.extend([temp_tx])
                    temp_ptx_list = self.__return_ptx_list(temp_tx)
                    if temp_ptx_list:
                        temp_tx = temp_ptx_list.pop()
                        ptx_list = temp_ptx_list
                    elif ptx_list:
                        temp_tx = ptx_list.pop()
                
                else:
                    break
    
        return tx_chain_list

    def __return_ptx_list(self,tx):
        self.curCore.execute(f"select ptx from TxIn where tx = {tx}")
        ptx_list = self.curCore.fetchall()
        ptx_list = [x[0] for x in ptx_list]
        return ptx_list

    def __is_sweeper(self, tx):
        self.curCore.execute(f"SELECT count(tx) from TxOut where tx =  ({str(tx)})")
        txout_count = self.curCore.fetchone()[0]

        self.curCore.execute(f"SELECT count(tx) from TxIn where tx =  ({str(tx)})")
        txin_count = self.curCore.fetchone()[0]

        if txout_count == 1 and txin_count > 1:
            return True
        else:
            return False
                
    def __return_out_list(self,tx):
        self.curCore.execute(f"select n from TxOut where tx = {tx}")
        n = self.curCore.fetchone()
        if n:
            n = n[0]
            self.curCore.execute(f"select tx from TxIn where ptx = {tx} and pn = {n}")
            tx = self.curCore.fetchone()
            if tx:
                tx = tx[0]
                self.curCore.execute(f"select tx from TxOut where tx = {tx} ")
                tx = self.curCore.fetchone()
                if tx:
                    tx = tx[0]
                    return tx
        
        else:
            return None
        



if __name__ == "__main__":
    # target_file = '220529_illegal_analysis_result.csv'
    # target_file = '220217_elliptic_addr.csv'

    block_start = 0
    block_end = 730000
    core_db_path = "dbv3-core.db"
    index_db_path ="dbv3-index.db"
    service_db_path = "dbv3-service.db"

    result = []
    f = open(f'220717_{block_start}_{block_end}.write.csv','a', newline='')
    wr = csv.writer(f)
    for i in tqdm(range(block_start-1, block_end, 10), desc = f"{block_start} Block Progress"):
        try:
            il = find_mixer(index_db_path, core_db_path, service_db_path, [block_start, i], tqdm_off=True)
            # result.extend(il.save_transaction)
            block_start = i
            for tx in il.save_transaction:
                wr.writerow([tx])
        except:
            wr.writerow([f"Program is down in started {block_start} block height"])
    f.close()
