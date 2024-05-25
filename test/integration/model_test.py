import sys,os
sys.path.append(os.path.dirname(os.path.abspath("__file__")))
if os.name=="nt":
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"\\src")
else:
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"/src")


#os.environ['DISPLAY'] = "localhost:10.0" 

from src.model.DataManager import DataManager
from src.presenter.Presenter import Presenter 
from src.viewer.Viewer import Viewer
from matplotlib import pyplot as plt
import matplotlib   
#matplotlib.use('QtAgg')
matplotlib.use('TkAgg')
#matplotlib.use('GTK3Agg') 
# Importing gc module
import gc

# Integration test
def createdb_test():
    dataManager = DataManager()
    # test create db
    dataManager.test_createdb()#

def filter_test():
    viewer = Viewer()
    presenter = Presenter(viewer=viewer)
    dataManager = DataManager(presenter=presenter ,use_redis=False)
    # # test filter
    # trs, dfs = dataManager.filter_trs_and_dfs(tr_name_substring='GMJuly2022_CELL018')
    # for tr in trs:
    #     print(tr)
    #     print(dfs)
    # dfs = dataManager.filter_dfs(tr_name_substring='GMJuly2022_CELL018')
    # for df in dfs:
    #     print(df.columns)
    cycle_stats = dataManager.filter_cycle_stats(tr_name_substring='GMJuly2022_CELL018')
    print(cycle_stats.cyc_end_datapoint_time)


def consistency_test():
    v = Viewer()
    p = Presenter(viewer=v)
    dataManager = DataManager(presenter=p ,use_redis=False)
    # test consistency
    dataManager.check_and_repair_consistency()

def process_cell_test():
    # test process_cell
    # umbl_cell_nums = [152097,152091,152051,152087,152042,152094,152045,152054,152039,152065,152041,152046,152057,152078,152033,152088,152036,152084,152085,152047,152072,152048]
    # cell_names  = ['UMBL2022FEB' +'_CELL' + f'{cell:03d}' for cell in umbl_cell_nums]
    #cell_names = ["GMFEB23S_CELL019"]
    #cell_names=["GMJULY2022_CELL"+f'{cell_num:03d}' for cell_num in range(1,120) ]
    cell_names=["GMFEB23S_CELL"+f'{cell_num:03d}' for cell_num in range(1,76) ]
    cell_names = ["GMFEB23S_CELL044"]
    #cell_names=["GMJULY2022_CELL"+f'{cell_num:03d}' for cell_num in [85,4,9,49,56,34,93,89,18,61,98,88,95 ] ] #85,4
    #cell_names=["GMJULY2022_CELL094"]
    #cell_names=["GMJULY2022_CELL086"]
    #cell_names=["GMJULY2022_CELL053"]
  #   cell_names=["GMJULY2022_CELL100"]
    #cell_names=["UMBL2022FEB_CELL152"+f'{cell_num:03d}' for cell_num in range(33,127) ]
# cell 27 has a problem...
    for cell_name in cell_names:
        viewer = Viewer()
        presenter = Presenter(viewer=viewer)
        dataManager = DataManager(presenter=presenter ,use_redis=False)

        # test process_cell

        cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt, junk = dataManager.process_cell(cell_name, reset=True)#, start_time='2022-07-01_00-00-00', end_time='2022-09-28_23-59-59')
    #    cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name, reset=True, start_time='2022-09-13_10-00-00', end_time='2022-09-24_10-00-00');#, reset=True)#)
        plt.show()
        # Returns the number of
        # objects it has collected
        # and deallocated
        collected = gc.collect()
        
        # Prints Garbage collector 
        # as 0 object
        print("Garbage collector: collected",
                "%d objects." % collected)


def process_tr_test():
    tr_name = 'GMJuly2022_CELL025_Test7B_1_P0C_5P0PSI_20230412_R0_CH055'
    tr_name = 'GMJuly2022_CELL085_RPT_1_P25C_25P0PSI_20220914_R0_CH030_20220914104354_36_4_6_2818579441'
    tr_name = 'GMJuly2022_CELL085_6b_1_P25C_25P0PSI_20220930_R0_CH030_20220930225513_36_4_6_2818579451'
    viewer = Viewer()
    presenter = Presenter(viewer=viewer)
    dataManager = DataManager(presenter=presenter ,use_redis=False)
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


def clean_unknown_project():
    v = Viewer()
    p = Presenter(viewer=v)
    dataManager = DataManager(presenter=p ,use_redis=False)
    dataManager.clean_unknown_project()

if __name__ == '__main__':
#    consistency_test()
   # clean_unknown_project()
    process_cell_test()
    #process_tr_test()