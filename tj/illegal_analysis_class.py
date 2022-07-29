import multiprocessing
import os
import numpy as np
import pandas as pd 
import sqlite3
from collections import Counter, defaultdict
from sklearn import preprocessing
from datetime import datetime
import bisect
from scipy.special import rel_entr, kl_div
from scipy.spatial import distance
from scipy.stats import entropy
from numpy.linalg import norm

from tqdm import tqdm

class illegal_analysis():
    def __init__(self, core_db_path, index_db_path, bitcoin_price_file_path):
        ### DB 연결
        self.conIndex = sqlite3.connect(f"./{index_db_path}")
        self.curIndex = self.conIndex.cursor()
        self.conCore = sqlite3.connect(f"./{core_db_path}")
        self.curCore = self.conCore.cursor()
        print(f"BitSQL DataBase Connect SUCCESS")
        ## 시세 가격
        self.price_data = pd.read_csv(f"./{bitcoin_price_file_path}", header = 0)
        print(f"Bitcoin Price Data Load SUCCESS")
        self.indexed_address_list = []
        self.tx_list = []
        self.addr_usd_hash = defaultdict(list)
        self.addr_tx_hash = defaultdict(list)
        self.addr_usd_sigma_value = defaultdict()
        self.addr_mean_derivation_value_hash = defaultdict(list)
        self.tx_unixtime_hash = defaultdict(set)
        self.tx_btc_price_hash = defaultdict(list)
        self.tx_datetime_hash = defaultdict(set)
        self.addr_usd_mean_value = defaultdict(list)
        self.ToManyTxAddrList = []

    def __get_indexed_address_using_origin_address(self, target_addr_list_file_path):
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
            indexed_addr = np.array(self.curIndex.fetchall()).T[0]
            if not indexed_addr.any():
                continue
            else:
                self.indexed_address_list.extend(list(indexed_addr))

        return self.indexed_address_list

        

    def _get_tx_list_using_indexed_address_list(self, target_addr_list_file_path):
        
        indexed_address_list = self.__get_indexed_address_using_origin_address(target_addr_list_file_path)
        # indexed_address_list= np.asarray(indexed_address_list, dtype = str)
        print(f"Find tx ...")
        drop_count = 0
        
        # 테스트
        n = 1999
        slicing_indexed_address_list = [indexed_address_list[i * n:(i + 1) * n] for i in range((len(indexed_address_list) + n - 1) // n )] 
        
        for addr_list in tqdm(slicing_indexed_address_list, desc = "Getting Addr's Tx List"):
            _addr_list = list(map(str, addr_list))
            
            sql = f"SELECT addr, tx FROM TxOut WHERE addr IN ({','.join(['?']*len(_addr_list))})"
            query = self.curCore.execute(sql, _addr_list)
            cols = [column[0] for column in query.description]
            results= pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
            self.addr_tx_hash.update({k: g["tx"].tolist() for k,g in results.groupby("addr")})
                        
        print(f"Don't have any Transactions Address Count : {drop_count}")
        print(f"Found {len(self.addr_tx_hash)} Address include TX")

        return self.addr_tx_hash

    def _convert_unixtime(self, date_time): 
        unixtime = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').timestamp() 
        return unixtime

    def using_date(self, start_day, end_day):
        start_day = start_day+" 00:00:00"
        end_day = end_day+" 23:59:59"
        start_unixtime = self._convert_unixtime(start_day)
        end_unixtime = self._convert_unixtime(end_day)
        print(f"Start, End : {start_day}, {end_day}, Converted UnixTime : {start_unixtime}, {end_unixtime}")
        return start_unixtime, end_unixtime

    def get_blk_number_list_using_blktime(self, start_day, end_day):
        start_unixtime, end_unixtime = self.using_date(start_day, end_day)
        self.curCore.execute(f"SELECT blk FROM BlkTime WHERE unixtime BETWEEN {start_unixtime} and {end_unixtime}")
        result = self.curCore.fetchall()
        result_block_number_list = np.array(result).T[0]
        
        return result_block_number_list

    def get_tx_list_using_blk_number_list(self, start_day, end_day):
        blk_number_list = self.get_blk_number_list_using_blktime(start_day, end_day)
        
        for blk in blk_number_list:
            self.curCore.execute(f"SELECT tx FROM BlkTx WHERE blk = {blk}")
            result = np.array(self.curCore.fetchall()).T[0]
            self.tx_list.extend(result)
        
        print(f"Found tx!, Count : {len(self.tx_list)}")
        return self.tx_list

    def __get_tx_unixtime(self, tx_list):
        
        blk_list = []
        unixtime_list = []

        sql=f"SELECT blk FROM BlkTx WHERE tx IN ({','.join(['?']*len(tx_list))})"
        
        tx_list = np.asarray(tx_list, dtype=str)
        
        if len(tx_list) >= 2000:
            unixtime_list = []
        else:
            sql=f"SELECT blk FROM BlkTx WHERE tx IN ({','.join(['?']*len(tx_list))})"
            self.curCore.execute(sql, tx_list)
            blk_list = np.array(self.curCore.fetchall()).T[0]
            blk_list = np.asarray(blk_list, dtype = str)


            for blk in blk_list:            
                sql=f"SELECT unixtime FROM BlkTime WHERE blk = {blk}"
                self.curCore.execute(sql)
                unixtime = np.array(self.curCore.fetchone()).T[0]
                unixtime_list.append(unixtime)

        return unixtime_list
    
    def convert_unixtime_to_datetime(self, tx_unixtime_hash):
        tx_datetime_hash = defaultdict(set)
        for tx, unixtime in tx_unixtime_hash.items():
            blk_datetime = datetime.fromtimestamp(int(unixtime))
            tx_datetime_hash[tx] = str(blk_datetime)[0:16]
        
        return tx_datetime_hash
        
    def __make_tx_unixtime_hash(self, addr_tx_hash):
        # tx, btc List, blk time
        tx_unixtime_hash = defaultdict(set)
        for addr, tx_list in tqdm(addr_tx_hash.items(), desc = "Getting Tx's Unixtime "): 
            unixtime_list = self.__get_tx_unixtime(tx_list)
            for tx, unixtime in zip(tx_list, unixtime_list):
                tx_unixtime_hash[tx] = int(unixtime)
                
        return tx_unixtime_hash

    def __find_nearest(self, unixtime_list, price_list, value):
        _near_index = bisect.bisect(unixtime_list, value)
        return price_list[_near_index-1]

    def __convert_btc_to_usd(self, tx_unixime_hash, btc_price_df):
        tx_btc_price_hash = defaultdict()
        total = len(tx_unixime_hash.items())
        outoftime = 0
        btc_unixtime_list = btc_price_df['unixtime'].values.tolist()
        btc_price_list = btc_price_df['price'].values.tolist()

        for tx, unixtime in tqdm(tx_unixime_hash.items(),desc = "Convert BTC To USD"):
            if unixtime > 1325343600:
                tx_btc_price_hash[int(tx)] = self.__find_nearest(btc_unixtime_list, btc_price_list, unixtime)
            else:
                outoftime += 1

        print(f"Out of unixtime( < 1325343600 ) TX's count : {outoftime}")
        return tx_btc_price_hash
        
    def calc_derivation_using_tx_list(self, target_addr_list_file_path):

        addr_tx_hash = self._get_tx_list_using_indexed_address_list(target_addr_list_file_path)

        print("Calculation start")
        self.tx_unixtime_hash = self.__make_tx_unixtime_hash(addr_tx_hash)
        print("Get unixtime success")
        # tx_datetime_hash = self.convert_unixtime_to_datetime(tx_unixtime_hash)
        self.tx_btc_price_hash = self.__convert_btc_to_usd(self.tx_unixtime_hash, self.price_data)
        print("Get tx's usd price")
        # bitcoin 시세 가격 자체를 unixtime으로 변경해야 함. 
           
        # print(tx_btc_price_hash) <- tx별로 btc 시세 저장되어있음.

        addr_usd_hash = defaultdict(list)
        
        for addr, tx_list in tqdm(addr_tx_hash.items(), desc = "Getting Target Value of Input Address"):
            tx_list.sort(reverse=True)
            if len(tx_list) > 100:
                tx_list = tx_list[0:100]


            for tx in tx_list:
                if int(tx) in self.tx_btc_price_hash.keys():
                    sql = f"SELECT Txout.btc FROM TxIn INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n WHERE TxIn.tx = {str(tx)}"
                    self.curCore.execute(sql)
                    sqlresult = self.curCore.fetchall()
                    if len(sqlresult) == 1: # txin이 1개 일 경우, 해당 주소로 입금된 금액만 확인
                        sql = f"select btc from TxOut where tx = {str(tx)} and addr = {str(addr)};"
                        self.curCore.execute(sql)
                        second_sqlresult = self.curCore.fetchone()
                        result = np.array(second_sqlresult)[0]
                        result *= float(self.tx_btc_price_hash[int(tx)])
                        
                        ### 220714 삭제
                        # addr_usd_hash[addr].extend([result])    

                    elif len(sqlresult) > 1: # txin이 1개 이상일 경우
                        # 같이 입금 된 애들도 많은가 확인해봐야 함.--> 그렇다면 자기한테 입금된 금액만 보셈
                        sql = f"select addr, btc from TxOut where tx = {str(tx)};"
                        # self.curCore.execute(sql)
                        # sqlresult = self.curCore.fetchall()
                        query = self.curCore.execute(sql)
                        cols = [column[0] for column in query.description]
                        results= pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
                        result = {k: g["btc"].tolist() for k,g in results.groupby("addr")}
                        
                        if len(result.keys()) > 1: # 다 대 다  구조일 경우, 같이 입금된 애가 여러개임                            
                            if len(result.keys()) < 5:
                                txout_btc_list = [x[0] for x in result.values()]
                                max_btc_value = max(txout_btc_list)
                                if result[addr][0] == max_btc_value:
                                    btc_results = np.array([float(x[0]) for x in sqlresult])
                                    btc_results *= float(self.tx_btc_price_hash[int(tx)])
                                    addr_usd_hash[addr].extend(btc_results)
                                    self.addr_usd_mean_value[addr].extend([np.mean(btc_results)])
                                else:
                                    continue
                                    btc_results = np.array(result[addr])
                                    btc_results *= float(self.tx_btc_price_hash[int(tx)])
                                    addr_usd_hash[addr].extend([btc_results.sum()]) # 해당 주소로 입금된 금액만 sum
                                                                    
                            else:
                                continue
                                btc_results = np.array(result[addr])
                                btc_results *= float(self.tx_btc_price_hash[int(tx)]) #################### 이거 수정해야 함. 3:2인 경우가 있음. 이 때는 입금 표준액을 보아ㅑ할듯
                                addr_usd_hash[addr].extend([btc_results.sum()]) # 해당 주소로 입금된 금액만 sum
                                
                        elif len(result.keys()) == 1: # 다 대 1 구조일 경우, 내가 처음 생각했던 구조
                            btc_results = np.array([float(x[0]) for x in sqlresult])
                            btc_results *= float(self.tx_btc_price_hash[int(tx)])
                            addr_usd_hash[addr].extend(btc_results)
                            self.addr_usd_mean_value[addr].extend([np.mean(btc_results)])
                else:
                    continue          

        print("Complete tx's balance to usd")
        self.addr_usd_hash = addr_usd_hash
        return addr_usd_hash

    def calc_sigma_value(self):

        for addr, usd_list in self.addr_usd_hash.items():
            if len(usd_list) > 1:
                derivation_value = np.std(usd_list)
                mean_value = np.mean(usd_list)

                sigma_value = [1, 1.1, 1.2, 1.3, 1.4, 1.5, 2, 3]
                sigma_result = []
                
                for value in sigma_value:
                    sigma_count = 0
                    for usd in usd_list:
                        if round(usd, 5) < round(mean_value +  value * derivation_value, 5) and round(usd, 5) > round(mean_value - value * derivation_value, 5):
                            sigma_count += 1
                    sigma_result.append(sigma_count)
                
                if max(sigma_result) == sigma_result[0]:
                    self.addr_usd_sigma_value[addr] = '1'
                elif max(sigma_result) == sigma_result[1]:
                    self.addr_usd_sigma_value[addr] = '1.1'
                
                sigma_result = np.array(sigma_result)
                
            else:
                continue

            self.addr_mean_derivation_value_hash[addr].append(mean_value)
            self.addr_mean_derivation_value_hash[addr].append(derivation_value)
            # self.addr_mean_derivation_value_hash[addr].extend(sigma_result*(100 / len(usd_list)))

        return self.addr_mean_derivation_value_hash

    def calc_jensen_shannon_value(self):
        # self.addr_usd_hash #여기 USD리스트 들어가있음
        # self.addr_usd_sigma_value # 1인거 찾아야함.
        self.addr_kld_hash = defaultdict()
        self.addr_mean_kld_hash = defaultdict()

        illegal_input_list = [10.084682964, 10.084682964, 10.084682964]

        self.addr_usd_sigma_value = sorted(self.addr_usd_sigma_value.items(), key = lambda item: item[1])
        # base_addr = self.addr_usd_sigma_value[0][0]
        # target_addr = self.addr_usd_sigma_value[len(self.addr_usd_sigma_value)-1][0]
        # target_addr = self.addr_usd_sigma_value[1][0]
        # print(self.addr_usd_hash[base_addr], self.addr_usd_hash[target_addr])

        # 히스토그램을 사용하는 것이 옳은 것인지 모르겠음
        
        for addr, usd_list in self.addr_usd_hash.items():
            b = np.array(np.histogram(np.array(usd_list))[-1])
            a = np.array(np.histogram(np.array(illegal_input_list))[-1])
            result = preprocessing.normalize([a, b])
            self.addr_kld_hash[addr] = sum(kl_div(result[0], result[1]))

            # self.addr_kld_hash[addr] = distance.jensenshannon(a,b)

        for addr, mean_list in self.addr_usd_mean_value.items():
            if len(mean_list) > 1:
                b = np.array(np.histogram(np.array(mean_list))[-1])
                a = np.array(np.histogram(np.array(illegal_input_list))[-1])
                result = preprocessing.normalize([a, b])
                self.addr_mean_kld_hash[addr] = sum(kl_div(result[0], result[1]))

            else:
                self.addr_mean_kld_hash[addr] = -1
                

        # 1. 부정행위 입력 usd 리스트 확인
        # 2. 부정행위 주소들 모두 1번과 거리 비교 --> 메트릭 다 저장
        # 3. 일반주소들 모두 1번과 비교 --> 메트릭 다 저장
        # 4. 2번 3번 비교
                
    
########  변수 입력  #############################################################################
core_db_path = "dbv3-core.db"
index_db_path ="dbv3-index.db"
bitcoin_price_file_path = 'illegal_analysis_bitcoin_price_unixtime.csv'
# target_addr_list_file_path = '220523_abuse_only_address.csv'
# target_addr_list_file_path = '220529_abuse_only_address_short.csv'
# target_addr_list_file_path = '220217_elliptic_addr.csv'
# target_addr_list_file_path = '220706_new_address.csv'
# target_addr_list_file_path = '220217_elliptic_addr_short.csv'

# target_addr_list_file_path = '220711_chainalysis_ransomware_address.csv'
# target_addr_list_file_path = '220711_chainalysis_Darknet_address.csv'
# target_addr_list_file_path = '220711_chainalysis_Darknet_address_short.csv'
# target_addr_list_file_path = '220712_MerchantChainalysis_address.csv'
# target_addr_list_file_path = '220714_upbit_address.csv'
# '220714_bithumb_address.csv'


for file in os.listdir('./address'):
    il = illegal_analysis(core_db_path, index_db_path, bitcoin_price_file_path)
    il.calc_derivation_using_tx_list(file)
    result = il.calc_sigma_value()

    target_name = file.split("_")

    saved_list = []

    for addr, [mean, std] in result.items():
        saved_list.append([addr, mean, std])

    saved_df = pd.DataFrame(saved_list)
    saved_df.columns = ['Address', 'Mean', 'Std']
    saved_df.to_csv(f"{target_name[0]}_{target_name[1]}_mean_std_result.csv", header = False, index = False)


    # il.calc_jensen_shannon_value()

    # target_name = file.split('_')

    # kld_df = (pd.DataFrame(list(il.addr_kld_hash.items()),columns=['Address', 'usd_list_kld']))
    # kld_mean_df = (pd.DataFrame(list(il.addr_mean_kld_hash.items()),columns=['Address', 'mean_kld']))
    # kld_df.to_csv(f'220721_{target_name[0]}_{target_name[1]}_kld_result.csv', header = False, index= False)
    # kld_mean_df.to_csv(f'220721_{target_name[0]}_{target_name[1]}_kld_mean_result.csv', header = False, index= False)
    

# count = len(result.keys())

# print(f"Calculated Address : {count}")

# save_list = []


# for addr, [mean_value, std_value, one_sigma, one_one_sigma, one_two_sigma, one_three_sigma, one_four_sigma, one_five_sigma, two_sigma, three_sigma] in result.items():
    
#     il.curIndex.execute(f"SELECT AddrID.addr FROM AddrID WHERE AddrID.id = {addr}")
#     origin_addr = il.curIndex.fetchone()[0]
    
#     save_list.append([origin_addr, mean_value, std_value, one_sigma, one_one_sigma, one_two_sigma, one_three_sigma, one_four_sigma, one_five_sigma, two_sigma, three_sigma])


# result = pd.DataFrame(save_list)

# result.columns = ['addr', 'mean_value', 'std_value', '1sigma', '1.1sigma', '1.2sigma', '1.3sigma', '1.4sigma', '1.5sigma','2sigma', '3sigma'] # value : stdvalue(다대1) 혹은 입금된 금액(1대1, 1대다, 다대다 구조)

# target_name_token = target_addr_list_file_path.split("_")
# target_name = target_name_token[1]
# result.to_csv(f'220713_illegal_analysis_{target_name}_result.csv', index = False)
# print(f'220713_illegal_analysis_{target_name}_result.csv saved')
# print(f"Total result Length : {len(result)}")

# one_sigma = result[(result['1sigma'] >= 90.0)]
# one_one_sigma = result[(result['1.1sigma'] >= 90.0)]
# one_two_sigma = result[(result['1.2sigma'] >= 90.0)]
# one_three_sigma = result[(result['1.3sigma'] >= 90.0)]
# one_four_sigma = result[(result['1.4sigma'] >= 90.0)]
# one_five_sigma = result[(result['1.5sigma'] >= 90.0)]
# two_sigma = result[(result['2sigma'] >= 90.0)]
# three_sigma = result[(result['3sigma'] >= 90.0)]


# print(f"More than 90% of deposits exist within 1 sigma range : {len(one_sigma)}, {len(one_sigma)/ len(result) * 100}")
# print(f"More than 90% of deposits exist within 1.1 sigma range : {len(one_one_sigma)}, {len(one_one_sigma)/ len(result) * 100}")
# print(f"More than 90% of deposits exist within 1.2 sigma range : {len(one_two_sigma)}, {len(one_two_sigma)/ len(result) * 100}")
# print(f"More than 90% of deposits exist within 1.3 sigma range : {len(one_three_sigma)}, {len(one_three_sigma)/ len(result) * 100}")
# print(f"More than 90% of deposits exist within 1.4 sigma range : {len(one_four_sigma)}, {len(one_four_sigma)/ len(result) * 100}")
# print(f"More than 90% of deposits exist within 1.5 sigma range : {len(one_five_sigma)}, {len(one_five_sigma)/ len(result) * 100}")
# print(f"More than 90% of deposits exist within 2 sigma range : {len(two_sigma)}, {len(two_sigma)/ len(result) * 100}")
# print(f"More than 90% of deposits exist within 3 sigma range : {len(three_sigma)}, {len(three_sigma)/ len(result) * 100}")