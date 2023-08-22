import sys
sys.path.insert(0, '/Users/yiliu/Documents/GitHub/UMBC_Data_Localization/UMBC_Data_Localization/src')


from src.model.DataManager import DataManager

# Integration test
def createdb_test():
    dataManager = DataManager()
    # test create db
    dataManager.test_createdb()

def updatedb_test():
    dataManager = DataManager()
    # test update db
    dataManager.test_updatedb()

def filter_test():
    dataManager = DataManager()
    # test filter
    trs, dfs = dataManager.filter_trs_and_dfs(device_id=3521)
    print(trs)
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


if __name__ == '__main__':
    # createdb_test()
    # updatedb_test()
    # filter_test()
    # process_cell_test()
    consistency_test()
    
