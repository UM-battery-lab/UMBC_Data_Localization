import sys,os
sys.path.append(os.path.dirname(os.path.abspath("__file__")))
if os.name=="nt":
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"\\src")
else:
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"/src")
print(sys.path)

# from multiprocessing import set_start_method
from multiprocess import get_context
#from multiprocessing import Pool
from multiprocess import Pool
from src.model.DataManager import DataManager
from src.presenter.Presenter import Presenter 
from src.viewer.Viewer import Viewer
import gc
from functools import partial

# Integration test
def present_cell(cell_numR):


    #newfunc= partial(present_cell,dataManager)        
    

    # test process_cell
    #try:
    #cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name, reset=True)#, start_time='2023-07-01_00-00-00', end_time='2023-07-28_23-59-59')
    #except Exception as e:
    #    print(e)
        # Returns the number of
    # objects it has collected
    # and deallocated
    cell_names=["GMJuly2022_CELL"+f'{cell_num:03d}' for cell_num in range(cell_numR,cell_numR+10) ]
    #cell_names=["GMFEB23S_CELL"+f'{cell_num:03d}' for cell_num in range(cell_numR,cell_numR+10) ]

    dataManager = DataManager(use_redis=False)
    def save_figs(figs, cell_name, time_name):
        dataManager.save_figs(figs, cell_name, time_name)
    presenter = Presenter()
    viewer = Viewer(call_back=save_figs)
    #cell_name = "UMBL2022FEB_CELL152051"
    presenter.attach(viewer)
    dataManager.attach(presenter)
    
    for cell_name in cell_names:
        cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = None, None, None, None
        # test process_cell

        cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name, reset=True);#, reset=True)#)
   
        # Returns the number of
        # objects it has collected
        # and deallocated
        collected = gc.collect()
        
        # Prints Garbage collector 
        # as 0 object
        print("Garbage collector: collected",
                "%d objects." % collected)

    
if (__name__ == '__main__'):

        #for i in range
    # for cell in range(31,32): # 75
    #     present_cell(cell)
    #set_start_method("spawn")
 

    #with Pool(16) as pool:
    with get_context("spawn").Pool(10) as pool:
    #pool=Pool(16) 
        result = pool.map(present_cell, range(0,111,10) )
# #            result = pool.map(present_cell, [26,44,16,52,50,86,54,41,45] )
        #result = pool.map(present_cell, range(1,40) )
        #result = pool.map(present_cell, range(30,76,2) )
        #result = pool.map(present_cell, range(1,75) )
        # result = pool.map(present_cell, range(40,80) ) 
        # result = pool.map(present_cell, range(80,105) )     
 #       result = pool.map(present_cell,[152097,152091,152051,152087,152042,152094,152045,152054,152039,152065,152041,152046,152057,152078,152033,152088,152036,152084,152085,152047,152072,152048])
    pool.close()
    pool.join()
    print(result)
    print('end')
