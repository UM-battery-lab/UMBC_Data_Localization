import sys,os
sys.path.append(os.path.dirname(os.path.abspath("__file__")))
if os.name=="nt":
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"\\src")
else:
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"/src")

from src.model.DataManager import DataManager
from src.presenter.Presenter import Presenter 
from src.viewer.Viewer import Viewer
from matplotlib import pyplot as plt
import matplotlib   
matplotlib.use('QtAgg')
#matplotlib.use('TkAgg')

# Integration test
def createdb_test():
    dataManager = DataManager()
    # test create db
    dataManager.test_createdb()#

def filter_test():
    dataManager = DataManager()
    # test filter
    trs, dfs = dataManager.filter_trs_and_dfs(tr_name_substring='GMJuly2022_CELL018')
    for tr in trs:
        print(tr)
        print(dfs)
    # dfs = dataManager.filter_dfs(tr_name_substring='GMJuly2022_CELL018')
    # for df in dfs:
    #     print(df.columns)

def consistency_test():
    dataManager = DataManager()
    # test consistency
    dataManager.check_and_repair_consistency()

def process_cell_test():
    # test process_cell
    #cell_name='GMFEB23S_CELL044'
    #'GMJuly2022_CELL015'
    #'UMBL2022FEB_CELL152051'
    cell_name='GMJuly2022_CELL901REF'
    cell_name='GMJuly2022_CELL073'
    cell_name='GMJuly2022_CELL018'

    #cell_name='GMJuly2022_CELL012'

    #cell_name='GMFEB23S_CELL067'


    dataManager = DataManager(use_redis=False)
    def save_figs(figs, cell_name, time_name):
        dataManager.save_figs(figs, cell_name, time_name, keep_open=True)
    presenter = Presenter()
    viewer = Viewer(call_back=save_figs)
    #cell_name = "UMBL2022FEB_CELL152051"

    dataManager.attach(presenter)
    presenter.attach(viewer)

    cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = None, None, None, None
    # test process_cell


    cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name, reset=True);#, reset=True)#, start_time='2023-07-01_00-00-00', end_time='2023-07-28_23-59-59')



# t[len(t)-1]
# 1691409656500.0
# t[0]
# 1666131009000.0
    #cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell('GMJuly2022_CELL901REF', reset=True)#901REF')

    print(cell_data)
    print(cell_data_vdf)
    print(cell_cycle_metrics)
    plt.show()

def process_tr_test():
    tr_name = 'GMJuly2022_CELL025_Test7B_1_P0C_5P0PSI_20230412_R0_CH055'
    dataManager = DataManager(use_redis=False)
    presenter = Presenter()
    viewer = Viewer()
    dataManager.attach(presenter)
    presenter.attach(viewer)
    cell_cycle_metrics, cell_data, cell_data_vdf = dataManager.process_tr(tr_name)
    print(cell_data)
    print(cell_data_vdf)
    print(cell_cycle_metrics)
    plt.show()

def read_csv_test():
    dataManager = DataManager()
    # test read_csv
    ccm_csv = dataManager.load_ccm_csv("GMJuly2022_CELL050")
    print(ccm_csv)
    with open("V:\\voltaiq_data\\Processed\\GMJuly2022\\output.csv", "w", encoding="utf-8") as f:
        f.write(ccm_csv)

def update_cycle_stats_test():
    dataManager = DataManager()
    # test update_cs
    dataManager.update_cycle_stats()

def sanity_check_test():
    dataManager = DataManager()
    # test sanity_check
    dataManager.sanity_check()

def duplicate_ccm_test():
    dataManager = DataManager()
    # test duplicate_ccm
    dataManager.duplicate_ccm()

def onetest():
    dataManager = DataManager()
    cell_cycle_metrics, cell_data, cell_data_vdf=dataManager.process_tr('GMJuly2022_CELL018_RPT_3_P25C_5P0PSI_20231004_R0_CH025_20231004101619_36_4_1_2818579523')

if __name__ == '__main__':
    # createdb_test()

    # filter_test()

    # process_cell_test()

#     process_cell_test()
#    #process_tr_test()
 #   consistency_test()
    onetest()
    # read_csv_test()
    #update_cycle_stats_test()
    # sanity_check_test()
#    duplicate_ccm_test()
