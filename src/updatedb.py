from src.model.DataManager import DataManager

def update_db():
    dataManager = DataManager()
    #dataManager._updatedb(project_name='GMFEB23S' ,start_before='2023-06-01_22-59-59')
    #dataManager._updatedb(device_id=5837)
    dataManager._updatedb()
if __name__ == '__main__':
    update_db()