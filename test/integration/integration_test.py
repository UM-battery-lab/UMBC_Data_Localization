import sys
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(os.path.dirname(current_directory))
src_directory = os.path.join(project_directory, "src")
sys.path.insert(0, project_directory)
sys.path.insert(0, src_directory)

from src.model.DataManager import DataManager
from src.presenter.Presenter import Presenter 
from src.viewer.Viewer import Viewer

# Integration test
def present_cell():
    dataManager = DataManager(use_redis=False)
    def save_figs(figs, cell_name):
        dataManager.save_figs(figs, cell_name)
    presenter = Presenter()
    viewer = Viewer(call_back=save_figs)
    cell_name = "GMJuly2022_CELL081"
    dataManager.attach(presenter)
    presenter.attach(viewer)

    # test process_cell
    cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = dataManager.process_cell(cell_name)
if __name__ == '__main__':
    present_cell()