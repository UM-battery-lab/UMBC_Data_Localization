from src.model.DataManager import DataManager

def update_db():
    dataManager = DataManager()
    dataManager._updatedb(start_before='2023-06-30_23-59-59')

if __name__ == '__main__':
    update_db()