import sys
sys.path.insert(0, '/Users/zihaoye/Documents/GitHub/umbc_sanity/UMBC_Data_Localization')
import pandas as pd
import re
import math

from src.model.DataManager import DataManager
from src.model.DataFetcher import DataFetcher

# TODO: Chaneg the path to correct path locally
file_path = "/Users/zihaoye/Documents/GitHub/umbc/GM_UMBL Formation Data and Fixtures - Neware Thermal Chamber Tracker.csv"
csv_path = "/Users/zihaoye/Documents/GitHub/umbc/wrong_newarefile.csv"

def sanity_check():
    df = pd.DataFrame(columns=['filename'])
    infopack = pd.read_csv(file_path) # GM_UMBL

    dataFetcher = DataFetcher()
    trs = dataFetcher.fetch_trs()

    print("The total neware file locally is: ")
    print(len(trs))

    for tr in trs:

        tags = tr.tags

        if(tags is None or "neware_xls_4000" not in tags):
            continue
        
        name = tr.name
        elements = name.split('_')

        try:
            # Access the desired elements using their indices
            # Elements from the file name
            cellID = elements[1]
            #testType = elements[2]
            # testDate = elements[6]
            # runNumber = elements[7]
            # ch_num = elements[8]
            racknumber = elements[10]
            Unit_ID = elements[11]
            chID = elements[12]

            nw_channel = Unit_ID + "-" + chID

            # Extract the cell number
            tmp = re.search(r'(\d+)', cellID)
            id = int(tmp.group(1))
            id = str(id)

            # Find the row(s) where the value in the specified column matches the value_to_find
            matching_row = infopack[infopack["Cell #"] == id]

            # Correct elements
            # cr_testType = "Test" + matching_row["Test Protocol"].iloc[0]

            num = matching_row["Running"].iloc[0]
            if math.isnan(num): 
                num = "0"
            cr_runumber = "R" + num

            cr_racknumber = matching_row["Neware rack"].iloc[0]
            cr_nwchannel = matching_row["Neware Channel"].iloc[0]

            # Q: this function only check the rack number and the channel number
            if(racknumber == cr_racknumber and nw_channel == cr_nwchannel):
                print(name + " is correct")
            else:
                new_data = pd.DataFrame({'filename': [name]})

                # Use pd.concat to append the data
                df = pd.concat([df, new_data], ignore_index=True)
            

        except KeyboardInterrupt:
            print("You cancel the operation!")
            break

        except:
            # if access failed, the name itself must be wrong
            new_data = pd.DataFrame({'filename': [name]})

            # Use pd.concat to append the data
            df = pd.concat([df, new_data], ignore_index=True)
            continue
        
    df.to_csv(csv_path)

sanity_check()