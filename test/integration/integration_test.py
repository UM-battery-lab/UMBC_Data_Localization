import sys,os
sys.path.append(os.path.dirname(os.path.abspath("__file__")))
if os.name=="nt":
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"\\src")
else:
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"/src")
print(sys.path)


from multiprocessing import Pool
from src.model.DataManager import DataManager
from src.presenter.Presenter import Presenter 
from src.viewer.Viewer import Viewer

# Integration test
def present_cell(cell_num):
 #   cell_name="GMJuly2022_CELL"+f'{cell_num:03d}'
    cell_name="GMFEB23S_CELL"+f'{cell_num:03d}'

    dataManager = DataManager(use_redis=False)
    def save_figs(figs, cell_name, time_name):
        dataManager.save_figs(figs, cell_name, time_name)
    presenter = Presenter()
    viewer = Viewer(call_back=save_figs)
    #cell_name = "UMBL2022FEB_CELL152051"

    dataManager.attach(presenter)
    presenter.attach(viewer)

    # test process_cell
    try:
        cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name, reset=True)#, start_time='2023-07-01_00-00-00', end_time='2023-07-28_23-59-59')
    except Exception as e:
        print(e)

if __name__ == '__main__':

        #for i in range
    #for cell in range(2,120):
    with Pool() as pool:
            result = pool.map(present_cell, range(1,122) )
    
