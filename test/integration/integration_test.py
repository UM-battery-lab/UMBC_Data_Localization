import sys
sys.path.insert(0, '/Users/yiliu/Documents/GitHub/UMBC_Data_Localization/UMBC_Data_Localization/src')


from src.model.DataManager import DataManager
from src.presenter.Presenter import Presenter 
from src.viewer.Viewer import Viewer

# Integration test
def present_cell():
    dataManager = DataManager()
    presenter = Presenter(dataManager)
    viewer = Viewer()
    cell_name = "GMJuly2022_CELL018"
    cell_data = presenter.get_measured_data_time(cell_name, start_time='2023-05-22_00-00-00', end_time='2023-07-22_23-59-59')
    viewer.plot_process_cell(cell_name, cell_data)
    cell_data = presenter.get_cycle_metrics_times(cell_name, start_time='2023-05-22_00-00-00', end_time='2023-07-22_23-59-59')
    viewer.plot_cycle_metrics_time(cell_name, cell_data)
    cell_data = presenter.get_cycle_metrics_AhT(cell_name, start_time='2023-05-22_00-00-00', end_time='2023-07-22_23-59-59')
    viewer.plot_cycle_metrics_AhT(cell_name, cell_data)
if __name__ == '__main__':
    present_cell()