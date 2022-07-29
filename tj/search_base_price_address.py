import bisect
import csv
from operator import index
import pandas as pd
import numpy as np
import sqlite3
from collections import defaultdict
from tqdm import tqdm
from check_tx_connect_mix_tx import Check_tx_connect_mixing_tx
from suspicious_addrid_to_addr import Conversion_addr

class find_price():
    def __init__(self, index_db_path, core_db_path, service_db_path, bitcoin_price_file_path, price_range:list, target_path, tqdm_off = False):
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
        self.price_range = price_range
        self.bitcoin_price_file_path = bitcoin_price_file_path
        self.target_path = target_path
        
        
        self.__get_indexed_address_using_origin_address(target_path)
        self._get_tx_list_using_indexed_address_list()
        self.__get_tx_block()
        self.__get__unixtime_using_tx_block()
        self.__get_tx_balance()
        
        self.__make_tx_btclist_to_usdlist()
        result_list = self.__check_tx_usd_mean(price_range)
        if result_list:
            self.__save_list(result_list)


    def __get_indexed_address_using_origin_address(self, target_addr_list_file_path):
        self.indexed_address_list = []
        target_addr = pd.read_csv(f"./address/{target_addr_list_file_path}", header = 0)
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
        
        for addr_list in tqdm(slicing_origin_addr, desc = "Origin Addr To Index Addr"):
            sql = f"SELECT AddrID.id FROM AddrID WHERE AddrID.addr IN ({','.join(['?']*len(addr_list))})"
            self.curIndex.execute(sql, addr_list)
            indexed_addr = self.curIndex.fetchall()
            if indexed_addr:
                indexed_addr = [x[0] for x in indexed_addr]
                self.indexed_address_list.extend(list(indexed_addr))

    def _get_tx_list_using_indexed_address_list(self):
        self.addr_tx_dict = defaultdict(list)
        self.tx_addr_dict = defaultdict(list)
        # self.indexed_address_list
        # indexed_address_list= np.asarray(indexed_address_list, dtype = str)
        print(f"Find tx ...")
        drop_count = 0
        
        # 테스트
        n = 1999
        slicing_indexed_address_list = [self.indexed_address_list[i * n:(i + 1) * n] for i in range((len(self.indexed_address_list) + n - 1) // n )] 
        
        for addr_list in tqdm(slicing_indexed_address_list, desc = "Getting Addr's Tx List"):
            _addr_list = list(map(str, addr_list))
            
            sql = f"SELECT addr, tx FROM TxOut WHERE addr IN ({','.join(['?']*len(_addr_list))})"
            query = self.curCore.execute(sql, _addr_list)
            cols = [column[0] for column in query.description]
            results= pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
            # self.addr_tx_dict.update({k: g["tx"].tolist() for k,g in results.groupby("addr")})
            self.tx_addr_dict.update({k: g["addr"].tolist() for k,g in results.groupby("tx")})
                        
    def _get_tx_using_block_range(self, block_range:list):
        latest_block = block_range[1]
        oldest_block = block_range[0]
        self.tx_block_dict = defaultdict()
        
        target_block_list = []
        
        for i in range(oldest_block, latest_block+1):
            target_block_list.append(i)

        for block in tqdm(target_block_list, desc = "Getting tx Using Block", disable= self.tqdm_off):
            sql = f"SELECT tx FROM BlkTx WHERE blk = {block}"
            self.curCore.execute(sql)
            tx_list = self.curCore.fetchall()
            
            if tx_list:
                tx_list = [x[0] for x in tx_list]
                for tx in tx_list:
                    self.tx_block_dict[tx] = block
                
    def __get_tx_block(self):
        self.tx_block_dict = defaultdict(list)
        self.block_tx_dict = defaultdict(list)
        # self.tx_addr_dict : tx의 addr 담겨있음.
        tx_list = list(self.tx_addr_dict.keys())
        
        n = 1999
        slicing_indexed_tx_list = [tx_list[i * n:(i + 1) * n] for i in range((len(tx_list) + n - 1) // n )]
        for tx_list in tqdm(slicing_indexed_tx_list, desc = "Getting Tx's Block"):
            _tx_list = tx_list
            sql = f"SELECT blk, tx FROM BlkTx WHERE tx IN ({','.join(['?']*len(_tx_list))})"
            query = self.curCore.execute(sql, _tx_list)
            cols = [column[0] for column in query.description]
            results= pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
            self.tx_block_dict.update({k: g["blk"].tolist() for k,g in results.groupby("tx")})
            self.block_tx_dict.update({k: g["tx"].tolist() for k,g in results.groupby("blk")})
            
    
    def __get_tx_balance(self):
        # self.block_tx_dict : block의 txlist 담겨있음
        # self.tx_block_dict : block의 txlist 담겨있음
        # self.addr_tx_dict : addr의 tx가 담겨있음.
        # self.tx_addr_dict : tx의 addr이 담겨있음.
        self.tx_btc_dict = defaultdict(list)
        tx_list = list(self.tx_block_dict.keys())
        
        n = 1999
        slicing_indexed_tx_list = [tx_list[i * n:(i + 1) * n] for i in range((len(tx_list) + n - 1) // n )]         
        
        for tx_list in slicing_indexed_tx_list:
            sql = f"SELECT TxIn.tx, Txout.btc FROM TxIn INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n WHERE TxIn.tx IN ({','.join(['?']*len(tx_list))})"
            query = self.curCore.execute(sql, tx_list)
            cols = [column[0] for column in query.description]
            results= pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
            self.tx_btc_dict.update({k: g["btc"].tolist() for k,g in results.groupby("tx")})
                        
    def __get__unixtime_using_tx_block(self):
        self.block_unixtime = defaultdict()
        blk_list = list(self.block_tx_dict.keys())
        blk_list.sort() 
        
        n = 1999
        slicing_indexed_blk_list = [blk_list[i * n:(i + 1) * n] for i in range((len(blk_list) + n - 1) // n )]         
        
        
        for blk_list in tqdm(slicing_indexed_blk_list,desc = "Get unix Time", disable=self.tqdm_off):
            sql = f"SELECT unixtime, blk FROM BlkTime WHERE blk IN ({','.join(['?']*len(blk_list))})"
            query = self.curCore.execute(sql, blk_list)
            cols = [column[0] for column in query.description]
            results= pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
            self.block_unixtime.update({k: g["unixtime"].tolist() for k,g in results.groupby("blk")})
                        
                
    def __make_tx_btclist_to_usdlist(self,):
        # self.block_tx_dict : block의 txlist 담겨있음
        # self.tx_block_dict : tx의 block이 담겨있음
        # self.block_unixtime : block의 unixtime 담겨있음
        # self.tx_btc_dict : tx의 btc_list들이 담겨있음.
        
        self.tx_usd_mean = defaultdict()
        btc_price_df = pd.read_csv(self.bitcoin_price_file_path, header = 0)
        usd_list = []
        btc_unixtime_list = btc_price_df['unixtime'].values.tolist()
        btc_price_list = btc_price_df['price'].values.tolist()
        
        for tx, btc_list in tqdm(self.tx_btc_dict.items(),desc = "tx's btclist to usd mean",disable=self.tqdm_off):
            base_block = self.tx_block_dict[tx]
            unixtime = self.block_unixtime[base_block[0]][0]
            if unixtime > 1325343600:
                market_price = self.__find_nearest(btc_unixtime_list, btc_price_list, unixtime)
            else:
                market_price = 0

            # 2시그마 범위 내에 있는 녀석들만 입력
            usd_list = np.array(btc_list) * market_price
            two_std = np.std(usd_list) * 2
            results = usd_list[np.where(np.logical_and(usd_list>(usd_list.mean() - two_std),usd_list<(usd_list.mean()+two_std)))]
            
            if len(usd_list) == len(results):            
                self.tx_usd_mean[tx] = usd_list.mean()

    def __find_nearest(self, unixtime_list, price_list, value):
        _near_index = bisect.bisect(unixtime_list, value)
        return price_list[_near_index-1]    
        
    def __check_tx_usd_mean(self, price_range:list):
        # self.tx_usd_mean : tx의 usd mean 담김
        tx_list = []
        for tx, usd_mean in tqdm(self.tx_usd_mean.items(),desc="Final Check Tx's Usd Mean",disable=self.tqdm_off):
            if usd_mean < price_range[1] and usd_mean > price_range[0]:
                tx_list.append(tx)
            else:
                continue
        return tx_list
    
    def __save_list(self, result_list:list):
        
        txid_list = []
        
        n = 1999
        slicing_indexed_tx_list = [result_list[i * n:(i + 1) * n] for i in range((len(result_list) + n - 1) // n )] 
        for tx_list in tqdm(slicing_indexed_tx_list, desc = "Getting Original Tx's list", disable=self.tqdm_off):
            _tx_list = list(map(str, tx_list))
            sql = f"SELECT txid FROM TxID WHERE id IN ({','.join(['?']*len(_tx_list))})"
            self.curIndex.execute(sql, _tx_list)
            results = self.curIndex.fetchall()
            if results:
                results = [x[0] for x in results]
                txid_list.extend(results)
        
        save_list = []
        for id, txid in zip(result_list, txid_list):
            save_list.append([id, txid])
            
        save_df = pd.DataFrame(save_list)
        save_df.columns = ['id','txid']
        
        file_names = self.target_path.split(".")[0]
        files = file_names.split("_")
        
        
        self.outfile_name = f"{self.price_range[0]}_to_{self.price_range[1]}_dollar_{files[0]}_{files[1]}_result.csv"
        save_df.to_csv(f"{self.price_range[0]}_to_{self.price_range[1]}_dollar_{files[0]}_{files[1]}_result.csv", index=False, header=False)
        
            
# bc1q7y0qslexnsd5ra6vwsnkyk5yfl8dnqznl0yqkw
# 692529
if __name__ == "__main__":
    target_path = 'chainalysis_ransomware_address.csv'
    core_db_path = "dbv3-core.db"
    index_db_path ="dbv3-index.db"
    service_db_path = "dbv3-service.db"
    bitcoin_price_file_path = 'illegal_analysis_bitcoin_price_unixtime.csv'
    target_mixing_tx_list_file = "0_to_730000_block_mixing_address.csv"
    target_price = [500, 700]
    
    fp = find_price(index_db_path, core_db_path, service_db_path, bitcoin_price_file_path, target_price,target_path, tqdm_off=False)
    file = fp.outfile_name
    check_tx = Check_tx_connect_mixing_tx(index_db_path, core_db_path, service_db_path, file, target_mixing_tx_list_file, False)
    conversion_addr = Conversion_addr(check_tx.outfile_name)