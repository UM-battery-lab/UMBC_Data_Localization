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
    trs, dfs = dataManager.filter_trs_and_dfs(device_id=3522)
    print(trs)
    print(dfs)
    trs = dataManager.filter_trs(start_time='2022-07-21_15-12-00')
    print(trs)
    dfs = dataManager.filter_dfs(tags=["neware_xls_4000"])
    print(dfs)

def process_cell_test():
    dataManager = DataManager()
    # test process_cell
    cell_data, cell_data_vdf, cell_cycle_metrics = dataManager.process_cell('Dewalt_H4A')
    print(cell_data)
    print(cell_data_vdf)
    print(cell_cycle_metrics)


if __name__ == '__main__':
    # createdb_test()
    # updatedb_test()
    # filter_test()
    process_cell_test()
    
