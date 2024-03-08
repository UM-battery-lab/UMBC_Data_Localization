import sys,os
sys.path.append(os.path.dirname(os.path.abspath("__file__")))
if os.name=="nt":
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"\\src")
else:
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"/src")
print(sys.path)

from multiprocessing import set_start_method
from multiprocessing import get_context
from multiprocessing import Pool
from src.model.DataManager import DataManager
from src.presenter.Presenter import Presenter 
from src.viewer.Viewer import Viewer
import gc

# Integration test
def present_cell(cell_num):


    cell_name="GMJuly2022_CELL"+f'{cell_num:03d}'
 #   cell_name="GMFEB23S_CELL"+f'{cell_num:03d}'

    viewer = Viewer()
    presenter = Presenter(viewer=viewer)
    dataManager = DataManager(presenter=presenter ,use_redis=False)

    # test process_cell
    #try:
    cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name, reset=True)#, start_time='2023-07-01_00-00-00', end_time='2023-07-28_23-59-59')
    #except Exception as e:
    #    print(e)
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
    with get_context("spawn").Pool(16) as pool:
    #pool=Pool(16)          
#         result = pool.map(present_cell, range(1,105) )
# #            result = pool.map(present_cell, [26,44,16,52,50,86,54,41,45] )
        result = pool.map(present_cell, range(1,40) )
        #result = pool.map(present_cell, range(30,76,2) )
     #   result = pool.map(present_cell, range(1,75) )
        #result = pool.map(present_cell, range(40,80) ) 
        #result = pool.map(present_cell, range(80,121) )     
    pool.close()
    pool.join()
    print(result)
    print('end')
