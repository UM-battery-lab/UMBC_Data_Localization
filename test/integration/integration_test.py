import sys,os
sys.path.append(os.path.dirname(os.path.abspath("__file__")))
if os.name=="nt":
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"\\src")
else:
    sys.path.append(os.path.dirname(os.path.abspath("__file__"))+"/src")
print(sys.path)


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