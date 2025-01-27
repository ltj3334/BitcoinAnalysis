from email import header
import pandas as pd 
from tqdm import tqdm
import sqlite3


class Conversion_addr():
    def __init__(self, file_name, original_file_name) -> None:
        df = pd.read_csv(file_name, header=0)
        
        addr_id = df['Addr']
        addr_id = addr_id.drop_duplicates()

        core_db_path = "dbv3-core.db"
        index_db_path ="dbv3-index.db"
        conIndex = sqlite3.connect(f"./{index_db_path}")
        curIndex = conIndex.cursor()
        conCore = sqlite3.connect(f"./{core_db_path}")
        curCore = conCore.cursor()
        indexed_address_list = []
        n = 1999
        slicing_indexed_addr_list = [addr_id[i * n:(i + 1) * n] for i in range((len(addr_id) + n - 1) // n )] 
        for addr_list in tqdm(slicing_indexed_addr_list, desc = "Getting Address List"):
            _addr_list = list(map(str, addr_list))
            sql = f"select addr from AddrID where id IN ({','.join(['?']*len(_addr_list))})"
            curIndex.execute(sql, _addr_list)
            addr_list = curIndex.fetchall()
            if addr_list:
                addr_list = [x[0] for x in addr_list]
                
                for addr in addr_list:
                    print(type(original_data['addr']))
                    is_illegal = original_data[original_data['addr'] == addr]
                    # is_illegal = original_data[is_illegal]
                    print(is_illegal)          
                    exit()
                    # indexed_address_list.append([addr, is_illegal])

        index_df = pd.DataFrame(indexed_address_list)
        index_df.columns = ['Address', 'is_illegal']
   
        
        target_name = file_name[:-11]

        index_df.to_csv(target_name+"_conversion_address.csv", header= False, index= False)
        
        
if __name__ == "__main__":
    original_file_name = '450_to_1400_dollar_chainalysis_ransomware_result.csv'
    file_name = '450_to_1400_dollar_chainalysis_ransomware_check_mixing_result.csv'
    ca = Conversion_addr(file_name, original_file_name)