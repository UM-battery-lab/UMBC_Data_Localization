import numpy as np
import pandas as pd
import time

from src.cycle.utils import find_matching_timestamp, max_min_cycle_data, test2df



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