import sys
# Change this path to your local path
sys.path.insert(0, '/Users/yiliu/Documents/GitHub/UMBC_Data_Localization/UMBC_Data_Localization/src')


from src.model.DataManager import DataManager
from src.presenter.Presenter import Presenter 
from src.viewer.Viewer import Viewer

# Integration test
def present_cell():
    dataManager = DataManager(use_redis=True)
    presenter = Presenter()
    viewer = Viewer()
    cell_name = "UMBL2022FEB_CELL152051"

    # test process_cell
    cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name)
    cell_data = presenter.get_measured_data_time(cell_cycle_metrics, cell_data, cell_data_vdf, start_time='2023-05-22_00-00-00', end_time='2023-07-22_23-59-59')
    viewer.plot_process_cell(cell_name, cell_data)
if __name__ == '__main__':
    present_cell()