from email import header
import pandas as pd 
from tqdm import tqdm
import sqlite3

file_name = '600_to_700_dollar_mixing_result.csv'
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
        indexed_address_list.extend([x[0] for x in addr_list])

index_df = pd.DataFrame(indexed_address_list)
index_df.columns = ['Address']

file = file_name.split(".")

index_df.to_csv(file[0]+"_address.csv", header= False, index= False)