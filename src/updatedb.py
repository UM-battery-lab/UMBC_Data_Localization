import sys
#sys.path.insert(0, '~/Documents/UMBC_Data_Localization/src/')
#sys.path.insert(0, '~/Documents/UMBC_Data_Localization/')
#sys.path.insert(0, '../src')


from src.model.DataManager import DataManager

def update_db():
    dataManager = DataManager()
    dataManager._updatedb()
#start_before='2023-06-30_23-59-59'
if __name__ == '__main__':
    update_db()