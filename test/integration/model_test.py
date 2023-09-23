import sys
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(os.path.dirname(current_directory))
src_directory = os.path.join(project_directory, "src")
sys.path.insert(0, project_directory)
sys.path.insert(0, src_directory)
from src.model.DataManager import DataManager

# Integration test
def createdb_test():
    dataManager = DataManager()
    # test create db
    dataManager.test_createdb()

def filter_test():
    dataManager = DataManager()
    # test filter
    trs, dfs = dataManager.filter_trs_and_dfs(tr_name_substring='UMBL2022FEB')
    for tr in trs:
        print(tr)
    # print(dfs)
    # dfs = dataManager.filter_dfs(tr_name_substring='GMJuly2022_CELL018')
    # for df in dfs:
    #     print(df.columns)

def consistency_test():
    dataManager = DataManager()
    # test consistency
    dataManager.check_and_repair_consistency()

def process_cell_test():
    dataManager = DataManager()
    # test process_cell
    cell_data, cell_data_vdf, cell_cycle_metrics = dataManager.process_cell('GMJuly2022_CELL034')
    print(cell_data)
    print(cell_data_vdf)
    print(cell_cycle_metrics)

def read_csv_test():
    dataManager = DataManager()
    # test read_csv
    ccm_csv = dataManager.load_ccm_csv("GMJuly2022_CELL018")
    print(ccm_csv)
    with open("output.csv", "w", encoding="utf-8") as f:
        f.write(ccm_csv)

def update_cycle_stats_test():
    dataManager = DataManager()
    # test update_cs
    dataManager.update_cycle_stats()

def sanity_check_test():
    dataManager = DataManager()
    # test sanity_check
    dataManager.sanity_check()

if __name__ == '__main__':
    # createdb_test()
    filter_test()
    # process_cell_test()
    # consistency_test()
    # read_csv_test()
    # update_cycle_stats_test()
    # sanity_check_test()
