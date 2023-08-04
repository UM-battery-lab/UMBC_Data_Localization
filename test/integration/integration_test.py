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
    data_dict = presenter.get_measured_data_time('Dewalt_H4A')
    viewer.plot_measured_data_time('Dewalt_H4A', data_dict)

if __name__ == '__main__':
    present_cell()