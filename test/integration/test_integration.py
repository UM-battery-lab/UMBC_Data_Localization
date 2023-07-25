import sys
sys.path.insert(0, '/Users/yiliu/Documents/GitHub/UMBC_Data_Localization/UMBC_Data_Localization/src')


import os
import pandas as pd

from src.fetch.fetch_data import fetch_trs, fetch_devs, get_dfs_from_trs
from src.save.save_data import create_dev_dic, save_test_data_update_dict
from src.read.find import find_trs_and_dfs
from src.read.read import load_record
from src.delete.delete import delete_test_data
from src.constants import ROOT_PATH


# Integration test
def integration_test():
    # Fetch data
    trs = fetch_trs()
    devs = fetch_devs()
    test_trs = trs[:50]
    dfs = get_dfs_from_trs(test_trs)

    # Save data
    devices_id_to_name = create_dev_dic(devs)
    save_test_data_update_dict(test_trs, dfs, devices_id_to_name)

    # Find and read data
    tr_paths, df_paths = find_trs_and_dfs(device_id=trs[0].device_id)
    for tr_path in tr_paths:
        assert os.path.exists(tr_path)
        tr = load_record(tr_path)
    for df_path in df_paths:
        assert os.path.exists(df_path)
        df = pd.read_pickle(df_path)
        assert isinstance(df, pd.DataFrame)
    
    # Find and read data
    tr_paths, df_paths = find_trs_and_dfs(device_name_substring=devices_id_to_name[trs[20].device_id])
    for tr_path in tr_paths:
        assert os.path.exists(tr_path)
        tr = load_record(tr_path)
    for df_path in df_paths:
        assert os.path.exists(df_path)
        df = pd.read_pickle(df_path)
        assert isinstance(df, pd.DataFrame)

if __name__ == '__main__':
    integration_test()
