import os
import time
import pickle
import pandas as pd
import numpy as np

from fetch.fetch_data import fetch_trs, fetch_devs

from cycle.process_cycler_data import process_cycler_data
from cycle.utils import test2df
from cycle.update_df import update_dataframe 
from cycle.filter import get_tests, sort_tests, filter_trs_new_data
from cycle.rpt import summarize_rpt_data
from cycle.expansion import process_cycler_expansion


def process_cell(device, filepath_rpt, filepath_ccm, filepath_cell_data, filepath_cell_data_vdf, cycle_id_lims, load_pickle = True, numFiles = 1000, print_filenames = False):
    """
    Process cycler data from a list of test records

    Parameters
    ----------
    device: Device object
        The device to be processed
    filepath_rpt: str
        The filepath to the cell report pickle file
    filepath_ccm: str
        The filepath to the cell cycle metrics pickle file
    filepath_cell_data: str
        The filepath to the cell data pickle file
    filepath_cell_data_vdf: str
        The filepath to the cell data vdf pickle file
    cycle_id_lims: list of ints
        The cycle number limits for charge, discharge, and total cycles
    load_pickle: bool, optional
        Whether to load the pickle files
    numFiles: int, optional
        The number of files to process
    print_filenames: bool, optional
        Whether to print the filenames as they are processed
    
    Returns
    -------
    DataFrame
        The processed data
    DataFrame
        The processed cycle metrics
    DataFrame
        The processed vdf data
    
    """
    
    trs = fetch_trs()

    #1. get and sort all cycler files for this cell 
    trs_neware = sort_tests(get_tests(device, trs, tag='neware_xls_4000'))
    trs_arbin = sort_tests(get_tests(device, trs, tag='arbin'))
    trs_biologic = sort_tests(get_tests(device, trs, tag='biologic'))
    trs_cycler = sort_tests(trs_neware + trs_arbin + trs_biologic)

    
    # 2. check if cell_data and cell_cycle_metrics pickle files exist. If so, add new test data to cell_data and cell_cycle_metrics
    load_new_data = [True for i in range(len(trs_neware))] # Initialization: indicates if the files were processed previously 
    if os.path.isfile(filepath_ccm) and os.path.isfile(filepath_cell_data) and load_pickle:
        # Load pickle file data and check for new data
        with open(filepath_ccm, 'rb') as f:
            cell_cycle_metrics = pickle.load(f)
            f.close()
        with open(filepath_cell_data, 'rb') as f:
            cell_data = pickle.load(f)
            f.close()
        # Make list of data files with new data to process
        trs_new_data = filter_trs_new_data(cell_cycle_metrics, trs_cycler)


        # For each new file, load the data and add it to the existing dfs
        for test in trs_new_data: 
            if print_filenames:
                print(test.name)
            # process test file
            cell_data_new, cell_cycle_metrics_new = process_cycler_data([test], cycle_id_lims=cycle_id_lims, numFiles = numFiles, print_filenames = print_filenames)

            # load test data to df and get start and end times
            df_test = test2df(test, test_trace_keys = ['aux_vdf_timestamp_datetime_0'], df_labels =['Time [s]'])
            file_start_time = df_test['Time [s]'].iloc[0]
            file_end_time = df_test['Time [s]'].iloc[-1] 

            # insert new timeseries test data into cell_data and recalculate AhT
            cell_data = update_dataframe(cell_data,cell_data_new, file_start_time, file_end_time)
            t = cell_data['Time [s]']
            I = cell_data['Current [A]']

            # insert new cycle metrics from test to cell_cycle_metrics  
            cell_cycle_metrics = update_dataframe(cell_cycle_metrics,cell_cycle_metrics_new, file_start_time, file_end_time)
            cell_cycle_metrics['Ah throughput [A.h]'] = cell_data['Ah throughput [A.h]'][(cell_data.discharge_cycle_indicator==True) | (cell_data.charge_cycle_indicator==True)]
    
    else: # if pickle file doesn't exist or load_pickle is False, process all cycling data
        trs_new_data = trs_cycler.copy()
        cell_data, cell_cycle_metrics = process_cycler_data(trs_new_data, cycle_id_lims=cycle_id_lims, numFiles = numFiles, print_filenames = print_filenames)
            
    #3. get and sort all vdf files for this cell 
    trs_vdf = sort_tests(get_tests(device,tag='vdf'))
    
    # 4. check if cell_data_vdf and cell_cycle_metrics pickle files exist. If so, list new test vdf data, else list all test vdf files.
    load_new_data_vdf = [True for i in range(len(trs_vdf))] # Initialization: indicates if the files were processed previously 
    
    if len(trs_vdf)==0: # make empty dfs for constrained cells 
        cell_data_vdf = pd.DataFrame(columns=['Time [s]','Expansion [-]', 'Expansion ref [-]', 'Temperature [degC]','cycle_indicator'])
        cell_cycle_metrics['Max cycle expansion [-]'] = np.nan
        cell_cycle_metrics['Min cycle expansion [-]'] = np.nan
        cell_cycle_metrics['Reversible cycle expansion [-]'] = np.nan
        trs_new_data_vdf = []
    elif os.path.isfile(filepath_cell_data_vdf) and load_pickle:
        with open(filepath_cell_data_vdf, 'rb') as f:
            cell_data_vdf = pickle.load(f)
            f.close()
        # Make list of data files with new data to process
        trs_new_data_vdf = filter_trs_new_data(cell_cycle_metrics, trs_vdf)
        if len(trs_new_data_vdf)>0: #ignore for constrained cells
            for test in trs_new_data_vdf:
                print(test.name)
                # process test file
                cell_data_vdf_new, cell_cycle_metrics_new = process_cycler_expansion([test], cell_cycle_metrics_new,numFiles = numFiles, print_filenames = print_filenames)

                # load test data to df and get start and end times
                df_test = test2df(test, test_trace_keys = ['h_datapoint_time'], df_labels =['Time [s]'])
                file_start_time = df_test['Time [s]'].iloc[0] 
                file_end_time = df_test['Time [s]'].iloc[-1] 

                # insert new timeseries test data into cell_data and cell_cycle_metrics
                cell_data_vdf = update_dataframe(cell_data_vdf,cell_data_vdf_new, file_start_time, file_end_time, update_AhT = False)
                cell_cycle_metrics = update_dataframe(cell_data,cell_cycle_metrics_new, file_start_time, file_end_time, update_AhT = False)

    else: # if pickle file doesn't exist or load_pickle is False, (re)process all expansion data
        trs_new_data_vdf = trs_vdf.copy()
        cell_data_vdf, cell_cycle_metrics = process_cycler_expansion(trs_vdf, cell_cycle_metrics, numFiles = numFiles, print_filenames = print_filenames)    

    
    # rearrange columns of cell_cycle_metrics for easy reading with data on left and others on right
    cols = cell_cycle_metrics.columns.to_list()
    move_idx = [c for c in cols if '[' in c] + [c for c in cols if '[' not in c] # Columns with data include '[' in the key
    cell_cycle_metrics = cell_cycle_metrics[move_idx]

    #5. save new data to pickle if there was new data
    if len(trs_new_data)>0:
        with open(filepath_ccm, 'wb') as f:
            pickle.dump(cell_cycle_metrics, f)
            f.close()
        with open(filepath_cell_data, 'wb') as f:
            pickle.dump(cell_data, f)
            f.close()
        with open(filepath_cell_data_vdf, 'wb') as f:
            pickle.dump(cell_data_vdf, f)
            f.close()    
        cell_rpt_data = summarize_rpt_data(cell_data, cell_data_vdf, cell_cycle_metrics)
        with open(filepath_rpt, 'wb') as f:
            pickle.dump(cell_rpt_data, f)
            f.close()
    return cell_cycle_metrics, cell_data, cell_data_vdf

def process_cycler_expansion(trs_vdf, cell_cycle_metrics, numFiles = 1000, t_match_threshold=60, print_filenames = False):
    # Combine vdf data into a single df
    cell_data_vdf = combine_cycler_expansion(trs_vdf, numFiles, print_filenames = print_filenames)
    
    # Find matching cycle timestamps from cycler data
    t_vdf = cell_data_vdf['Time [s]']
    exp_vdf = cell_data_vdf['Expansion [-]']
    cycle_timestamps = cell_cycle_metrics['Time [s]'][cell_cycle_metrics.cycle_indicator==True]
    t_cycle_vdf, cycle_idx_vdf, matched_timestamp_indices = find_matching_timestamp(cycle_timestamps, t_vdf, t_match_threshold=10)  

    # add cycle indicator. These should align with cycles timestamps previously defined by cycler data
    cell_data_vdf['cycle_indicator'] = list(map(lambda x: x in cycle_idx_vdf, range(len(cell_data_vdf))))
    
    # find min/max expansion
    cycle_idx_vdf_minmax = [i for i in cycle_idx_vdf if i is not np.nan]
    cycle_idx_vdf_minmax.append(len(t_vdf)-1) #append end
    exp_max, exp_min = max_min_cycle_data(exp_vdf, cycle_idx_vdf_minmax)
    exp_rev = np.subtract(exp_max,exp_min)

    # save data to dataframe: initialize with nan and fill in timestamp-matched values
    discharge_cycle_idx = list(np.where(cell_cycle_metrics.cycle_indicator==True)[0])
    cell_cycle_metrics['Time vdf [s]'] = [np.nan]*len(cell_cycle_metrics)
    cell_cycle_metrics['Min cycle expansion [-]'] = [np.nan]*len(cell_cycle_metrics)
    cell_cycle_metrics['Max cycle expansion [-]'] = [np.nan]*len(cell_cycle_metrics)
    cell_cycle_metrics['Reversible cycle expansion [-]'] = [np.nan]*len(cell_cycle_metrics)
    for i,j in enumerate(matched_timestamp_indices):
        cell_cycle_metrics.loc[discharge_cycle_idx[j], 'Time vdf [s]'] = t_cycle_vdf[i]
        cell_cycle_metrics.loc[discharge_cycle_idx[j], 'Min cycle expansion [-]'] = exp_min[i]
        cell_cycle_metrics.loc[discharge_cycle_idx[j], 'Max cycle expansion [-]'] = exp_max[i]
        cell_cycle_metrics.loc[discharge_cycle_idx[j], 'Reversible cycle expansion [-]'] = exp_rev[i]
        
    # also add timestamps for charge cycles
    charge_cycle_idx = list(np.where(cell_cycle_metrics.charge_cycle_indicator==True)[0])
    charge_cycle_timestamps = cell_cycle_metrics['Time [s]'][cell_cycle_metrics.charge_cycle_indicator==True]
    t_charge_cycle_vdf, charge_cycle_idx_vdf, matched_charge_timestamp_indices = find_matching_timestamp(charge_cycle_timestamps, t_vdf, t_match_threshold=10)
    for i,j in enumerate(matched_charge_timestamp_indices):
        cell_cycle_metrics.loc[charge_cycle_idx[j], 'Time vdf [s]'] = t_charge_cycle_vdf[i]

    return cell_data_vdf, cell_cycle_metrics

# PROCESS NEWARE VDF DATA
# Reads in data from the last "numFiles" files in the "trs_vdf" and concatenates them into a long dataframe. Then looks for corresponding cycle start/end timestamps in vdf time. 
# Finally, it'll calculate the min, max, and reversible expansion for each cycle. 

def combine_cycler_expansion(trs_vdf, numFiles = 1000, print_filenames = False):
    # concatenate vdf data frames for last numFiles files
    frames_vdf =[]
    # For each vdf file...
    for test_vdf in trs_vdf[0:min(len(trs_vdf), numFiles)]:
        try:
            # Read in timeseries data from test and formating into dataframe. Remove rows with expansion value outliers.
            if print_filenames:
                print(test_vdf.name)
            # df_vdf = test2df(test_vdf, test_trace_keys = ['aux_vdf_timestamp_datetime_0','aux_vdf_ldcsensor_none_0', 'aux_vdf_ldcref_none_0', 'aux_vdf_ambienttemperature_celsius_0', 'aux_vdf_temperature_celsius_0'], df_labels =['Time [s]','Expansion [-]', 'Expansion ref [-]', 'Amb Temp [degC]', 'Temperature [degC]'])
            df_vdf = test2df(test_vdf, test_trace_keys = ['aux_vdf_timestamp_datetime_0','aux_vdf_ldcsensor_none_0', 'aux_vdf_ldcref_none_0', 'aux_vdf_ambienttemperature_celsius_0'], df_labels =['Time [s]','Expansion [-]', 'Expansion ref [-]','Temperature [degC]'])
            df_vdf = df_vdf[(df_vdf['Expansion [-]'] >1e1) & (df_vdf['Expansion [-]'] <1e7)] #keep good signals 
            df_vdf['Temperature [degC]'] = np.where((df_vdf['Temperature [degC]'] >= 200) & (df_vdf['Temperature [degC]'] <250), np.nan, df_vdf['Temperature [degC]']) 
            # df_vdf['Amb Temp [degC]'] = np.where((df_vdf['Amb Temp [degC]'] >= 200) & (df_vdf['Amb Temp [degC]'] <250), np.nan, df_vdf['Amb Temp [degC]']) 
            frames_vdf.append(df_vdf)
        except: #Tables are different Length, cannot merge
            pass
        time.sleep(0.1) 
    
    # Combine vdf data into a single df and reset the index 
    cell_data_vdf = pd.concat(frames_vdf).sort_values(by=['Time [s]'])
    cell_data_vdf.reset_index(drop=True, inplace=True)

    return cell_data_vdf
    
    # 'aux_vdf_drivecurrent_none_0', 'Drive current [-]'