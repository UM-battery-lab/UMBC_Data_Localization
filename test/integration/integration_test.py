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
    dataManager.attach(presenter)
    presenter.attach(viewer)

    # test process_cell
    cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name)
if __name__ == '__main__':
    present_cell()