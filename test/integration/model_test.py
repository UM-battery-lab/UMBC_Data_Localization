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
    # umbl_cell_nums = [152097,152091,152051,152087,152042,152094,152045,152054,152039,152065,152041,152046,152057,152078,152033,152088,152036,152084,152085,152047,152072,152048]
    # umbl_cells = ['UMBL2022FEB' +'_CELL' + f'{cell:03d}' for cell in umbl_cell_nums]
    cell_names = ["UMBL2022FEB_CELL152087"]
    cell_names=["GMJuly2022_CELL049"]
    cell_names=['GMFEB23S_CELL069']
    # cell_names=["GMJuly2022_CELL"+f'{cell_num:03d}' for cell_num in range(1,105) ]
    cell_names=["GMFEB23S_CELL"+f'{cell_num:03d}' for cell_num in range(0,77) ]
    for cell_name in cell_names:
        dataManager = DataManager(use_redis=False)
        def save_figs(figs, cell_name, time_name):
            dataManager.save_figs(figs, cell_name, time_name, keep_open=True)
        presenter = Presenter()
        viewer = Viewer(call_back=save_figs)
        

        dataManager.attach(presenter)
        presenter.attach(viewer)

        cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = None, None, None, None
        # test process_cell


        cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt,junk = dataManager.process_cell(cell_name, reset=True);#, reset=True)#, start_time='2023-07-01_00-00-00', end_time='2023-07-28_23-59-59')
    #    cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name, reset=True, start_time='2022-09-13_10-00-00', end_time='2022-09-24_10-00-00');#, reset=True)#)
        

 
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
    dataManager = DataManager(use_redis=False)
    presenter = Presenter()
    viewer = Viewer()
    dataManager.attach(presenter)
    presenter.attach(viewer)
    #cell_cycle_metrics, cell_data, cell_data_vdf=dataManager.process_tr('GMJuly2022_CELL018_RPT_3_P25C_5P0PSI_20231004_R0_CH025_20231004101619_36_4_1_2818579523')
    #cell_cycle_metrics, cell_data, cell_data_vdf=dataManager.process_tr('GMFEB23S_CELL069_RPT_4_P25C_P15P0PSI_20230419_R0-01-026')
    #    cell_cycle_metrics, cell_data, cell_data_vdf=dataManager.process_tr('GMFEB23S_CELL022_Test8soc2080-Cby3-n100_1_P25C_15P0PSI_20230720_R0-01-027')
#    GMFEB23S_CELL075_Test8-soc2080-Cby3-n100_1_P25C_15P0PSI_20231022_R0-01-031
    #GMJuly2022_CELL104_Test7A_1_P45C_5P0PSI_20230310_R0_CH094
    #cell_cycle_metrics, cell_data, cell_data_vdf=dataManager.process_tr('GMJuly2022_CELL104_Test7A_1_P45C_5P0PSI_20230327_R0_CH094_20230327233233_34_2_6_2818580233')
    
    #cell_cycle_metrics, cell_data, cell_data_vdf=dataManager.process_tr('GMJuly2022_CELL104_Test7A_1_P45C_5P0PSI_20230207_R0_CH094_20230207101259_34_2_6_2818580218')
    # cell_cycle_metrics, cell_data, cell_data_vdf=dataManager.process_tr('GMJuly2022_CELL104_RPT_3_P25C_5P0PSI_20230310_R0_CH094_20230310163917_34_2_6_2818580224')

    cell_cycle_metrics, cell_data, cell_data_vdf=dataManager.process_tr('GMJuly2022_CELL085_RPT_1_P25C_25P0PSI_20220914_R0_CH030_20220914104354_36_4_6_2818579441')

    plt.show()

if __name__ == '__main__':
    # createdb_test()

    # filter_test()

    process_cell_test()
    #process_cell_test()
#    #process_tr_test()
#    consistency_test()
#    onetest()
    # read_csv_test()
    #update_cycle_stats_test()
 #   sanity_check_test()
#    duplicate_ccm_test()
