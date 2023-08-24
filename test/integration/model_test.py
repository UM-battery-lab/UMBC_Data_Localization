import sys
# Change this path to your local path
sys.path.insert(0, '/Users/yiliu/Documents/GitHub/UMBC_Data_Localization/UMBC_Data_Localization/src')


from src.model.DataManager import DataManager


def filter_test():
    dataManager = DataManager()
    # test filter
    # trs, dfs = dataManager.filter_trs_and_dfs(device_id=3521)
    # print(trs)
    # print(dfs)
    dfs = dataManager.filter_dfs(tr_name_substring="UMBL2022FEB_CELL152051", start_time='2023-06-22_16-00-00')
    for df in dfs:
        print(df.columns)

def process_cell_test():
    dataManager = DataManager()
    # test process_cell
    cell_data, cell_data_vdf, cell_cycle_metrics = dataManager.process_cell('GMJuly2022_CELL034')
    print(cell_data)
    print(cell_data_vdf)
    print(cell_cycle_metrics)


if __name__ == '__main__':
    filter_test()
    # process_cell_test()

