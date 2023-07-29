import pandas as pd 
import numpy as np
import time
from scipy import integrate
from scipy.signal import find_peaks, medfilt
from itertools import compress

from cycle.utils import find_cycle_idx, match_charge_discharge, test2df


CRED = "\033[41m" #red (terminal color for print statement) 
CEND = "\033[0m" #reset all formatting



def combine_cycler_data(trs_cycler, cycle_id_lims, numFiles=1000, print_filenames = False, last_AhT = 0):
    """
    Combine cycler data from multiple files into a single dataframe.
    PROCESS CYCLER DATA.
    Concatenate neware data frames, identify cycles based on rests (I=0), then calculate min and max voltage and temperature for each cycle.
    If you only want the latest test, set numFiles = 1. Assumes test files in trs_cycler are sorted with trs_cycler[0] being the latest.

    Parameters
    ----------
    trs_cycler: list of TestRecord objects
        The list of test records to be processed
    cycle_id_lims: dict
        Dictionary of cycle identification thresholds for different test types.
    numFiles: int, optional
        Number of files to process. Default is 1000.
    print_filenames: bool, optional
        Print the name of each file as it is processed. Default is False.
    last_AhT: float, optional   
        Ah throughput from last file. Default is 0.
    
    Returns
    -------
    cell_data: dataframe
        Dataframe of all cycler data from the specified files.
    cell_cycle_metrics: dataframe
        Dataframe of cycle metrics from the specified files.
    """

    # Appends data from each data file to a dataframe
    frames =[]
    test_types = list(cycle_id_lims.keys())

    # For each data file...
    for test in trs_cycler[0:min(len(trs_cycler), numFiles)]:
        try: 
            # 1. Load data from each data file to a dataframe. Update AhT and ignore unplugged thermocouple values. For RPTs, convert t with ms.
            isRPT =  ('RPT').lower() in test.name.lower() or ('EIS').lower() in test.name.lower() 
            isFormation = ('_F').lower() in test.name.lower() and not ('_FORMTAP').lower() in test.name.lower() 

            # 1a. for arbin and biologic files
            if ('arbin' in test.tags) or ('biologic' in test.tags): 
                test_trace_keys_arbin = ['h_datapoint_time','h_test_time','h_current', 'h_potential', 'c_cumulative_capacity', 'h_step_index']
                df_labels_arbin = ['Time [s]','Test Time [s]', 'Current [A]', 'Voltage [V]', 'Ah throughput [A.h]', 'Step index']
                test_data = test2df(test,test_trace_keys_arbin,df_labels_arbin, ms = isRPT)
                test_data['Temperature [degC]'] = [np.nan]*len(test_data) # make arbin tables with same columns as neware files
            # 1b. for neware files
            elif 'neware_xls_4000' in test.tags: 
                test_data = test2df(test, ms = isRPT)
                test_data['Temperature [degC]'] = np.where((test_data['Temperature [degC]'] >= 200) & (test_data['Temperature [degC]'] <250), np.nan, test_data['Temperature [degC]']) 
            
            # 2. Reassign to variables
            t = test_data['Time [s]']
            I = test_data['Current [A]']
            V = test_data['Voltage [V]'] 
            T = test_data['Temperature [degC]'] 
            step_idx = test_data['Step index']

            # 3. Calculate AhT 
            if 'neware_xls_4000' in test.tags and isFormation:  
                # 3a. From integrating current.... some formation files had wrong units
                AhT_calculated = integrate.cumtrapz(abs(I), (t-t[0]).dt.total_seconds())/3600 + last_AhT
                AhT_calculated = np.append(AhT_calculated,AhT_calculated[-1]) # repeat last value to make AhT the same length as t
                test_data['Ah throughput [A.h]'] = AhT_calculated
                # test_data['Ah throughput [A.h]'] = test_data['Ah throughput [A.h]']/1e6 + last_AhT # add last AhT value (if using scaled cycler cummulative capacity. Doesn't solve all neware formation AhT issues...)
                AhT = test_data['Ah throughput [A.h]']
            else:
                # 3b. From cycler cumulative capacity...
                test_data['Ah throughput [A.h]'] = test_data['Ah throughput [A.h]'] + last_AhT # add last AhT value (if using cycler cummulative capacity)
                AhT = test_data['Ah throughput [A.h]']
                
            # 3c. update AhT from last file
            AhT = test_data['Ah throughput [A.h]']
            last_AhT = AhT.iloc[-1] #update last AhT value for next file

            # 4. Change cycle filtering thresholds by test type and include the idx at the end of the file in case cell is still cycling.
            # Search for test type in test name. If there's no match, use the default settings 
            lims={}
            for test_type in test_types: # check for test types with different filters (e.g. RPT, F, EIS)
                if (test_type).lower() in test.name.lower(): 
                    lims = cycle_id_lims[test_type]
                    if isRPT:
                        test_protocol = 'RPT' #EIS -> RPT
                    else:
                        test_protocol = test_type
                if len(lims) == 0: #default
                    lims = cycle_id_lims['CYC']
                    test_protocol = 'CYC'
            V_max_cycle = lims['V_max_cycle']
            V_min_cycle = lims['V_min_cycle']
            dAh_min = lims['dAh_min']
            dt_min = lims['dt_min']

            # 5. Find indices for cycles in file
            if isFormation and 'arbin' in test.tags: # find peaks in voltage where I==0, ignore min during hppc
                peak_prominence = 0.1
                trough_prominence = 0.1
                discharge_start_idx_file, _ = find_peaks(medfilt(V[I==0], kernel_size = 101),prominence = peak_prominence)
                discharge_start_idx_file = test_data[I==0].iloc[discharge_start_idx_file].index.to_list()
                charge_start_idx_file,_ = find_peaks(-medfilt(V[I==0], kernel_size = 101),prominence = trough_prominence, height = (None, -2.7)) # height to ignore min during hppc
                charge_start_idx_file = test_data[I==0].iloc[charge_start_idx_file].index.to_list()
                charge_start_idx_file.insert(0, 0)
                charge_start_idx_file.insert(len(charge_start_idx_file), len(V)-1)
                charge_start_idx_file, discharge_start_idx_file = match_charge_discharge(np.array(charge_start_idx_file), np.array(discharge_start_idx_file))
            else: # find I==0 and filter out irrelevant points
                charge_start_idx_file, discharge_start_idx_file = find_cycle_idx(t, I, V, AhT, step_idx, V_max_cycle = V_max_cycle, V_min_cycle = V_min_cycle, dt_min = dt_min, dAh_min= dAh_min)
                try: # won't work for half cycles (files with only charge or only discharge)
                    charge_start_idx_file, discharge_start_idx_file = match_charge_discharge(charge_start_idx_file, discharge_start_idx_file)
                except:
                    pass

            # 6. Add aux cycle indicators to df. Column of True if start of a cycle, otherwise False. Set default cycle indicator = charge start 
            file_with_capacity_check = isRPT or isFormation 
            test_data['discharge_cycle_indicator'] = [False]*len(test_data)
            test_data['charge_cycle_indicator'] = [False]*len(test_data)
            test_data['capacity_check_indicator'] = [False]*len(test_data)

            test_data.loc[discharge_start_idx_file, 'discharge_cycle_indicator'] = True
            test_data.loc[charge_start_idx_file, 'charge_cycle_indicator'] = True
            test_data['cycle_indicator'] = test_data['charge_cycle_indicator'] # default cycle = charge start 
            if file_with_capacity_check:
                test_data.loc[charge_start_idx_file, 'capacity_check_indicator'] = True

            # 6a. Add test type and test name to test_data
            test_data['Test type'] = [' ']*len(test_data)
            test_data['Test name'] = [' ']*len(test_data)
            test_data.loc[np.concatenate((discharge_start_idx_file,charge_start_idx_file)), 'Test type'] = test_protocol
            test_data.loc[np.concatenate((discharge_start_idx_file,charge_start_idx_file)), 'Test name'] = test.name

            # 6b. identify subcycle type. For extracting HPPC and C/20 dis/charge data later. 
            test_data['Protocol'] = [np.nan]*len(test_data)
            file_cell_cycle_metrics = test_data[(test_data.charge_cycle_indicator==True) | (test_data.discharge_cycle_indicator==True)]

            for i in range(0,len(file_cell_cycle_metrics)):
                t_start = file_cell_cycle_metrics['Time [s]'].iloc[i]
                if i == len(file_cell_cycle_metrics)-1: # if last subcycle, end of subcycle = end of file 
                    t_end = test_data['Time [s]'].iloc[-1]
                else: # end of subcycle = start of next subcycle
                    t_end = file_cell_cycle_metrics['Time [s]'].iloc[i+1]
                t_subcycle = test_data['Time [s]'][(t>t_start) & (t<t_end)]
                I_subcycle = test_data['Current [A]'][(t>t_start) & (t<t_end)]
                data_idx = file_cell_cycle_metrics.index.tolist()[i]
                if file_with_capacity_check:
                    if len(np.where(np.diff(np.sign(I_subcycle)))[0])>10: # hppc: ID by # of types of current sign changes (threshold is arbitrary)
                        test_data.loc[data_idx,'Protocol'] = 'HPPC'
                    elif (t_end-t_start).total_seconds()/3600 >8 and  np.mean(I_subcycle) > 0: # C/20 charge: longer than 8 hrs and mean(I)>0. Will ID C/10 during formation as C/20...
                        test_data.loc[data_idx,'Protocol'] = 'C/20 charge'
                    elif (t_end-t_start).total_seconds()/3600 > 8 and  np.mean(I_subcycle) < 0: # C/20 discharge: longer than 8 hrs and mean(I)<0.Will ID C/10 during formation as C/20...
                        test_data.loc[data_idx,'Protocol'] = 'C/20 discharge'
                    # print('Avg I: ' + str(np.mean(I_subcycle)) + '   Duration: ' +str(round((t_end-t_start).total_seconds()/3600,2))  + '    '+ str(test_data.loc[data_idx]['Protocol']))


            # 7. Add to list of dfs where each element is the resulting df from each file.
            if print_filenames: # for debugging
                print(test.name + '   Cycles: ' + str(len(charge_start_idx_file)) + '   AhT: ' + str(round(AhT.iloc[-1],2)))
            frames.append(test_data)
        
        except Exception as e: # Error: Tables are Different Length, cannot merge or corrupted files
            print('\033[91m'+test.name + ' (' + str(e) + ' )'+CEND)

        time.sleep(0.1) 
    # Combine cycling data into a single df and reset the index
    cell_data = pd.concat(frames)
    cell_data.reset_index(drop=True, inplace=True)

    # Get cycle indices from combined df originally identified from individual tests (with lims based on test type) 
    discharge_start_idx_0 = np.array(list(compress(range(len(cell_data['discharge_cycle_indicator'])), cell_data['discharge_cycle_indicator'])))
    charge_start_idx_0 = np.array(list(compress(range(len(cell_data['charge_cycle_indicator'])), cell_data['charge_cycle_indicator'])))
    capacity_check_idx_0 = np.array(list(compress(range(len(cell_data['capacity_check_indicator'])), cell_data['capacity_check_indicator'])))
    
    # Filter cycle indices again to match every discharge and charge index. Set default cycle index to charge start
    if len((discharge_start_idx_0)>1) and (len(charge_start_idx_0)>1):
        charge_start_idx, discharge_start_idx = match_charge_discharge(charge_start_idx_0, discharge_start_idx_0) 
        cycle_idx = charge_start_idx
        # Remove cycle indices that were filtered out
        removed_charge_cycle_idx = list(set(charge_start_idx_0).symmetric_difference(set(charge_start_idx)))
        cell_data.loc[removed_charge_cycle_idx,'charge_cycle_indicator'] = False
        removed_discharge_cycle_idx = list(set(discharge_start_idx_0).symmetric_difference(set(discharge_start_idx)))
        cell_data.loc[removed_discharge_cycle_idx,'discharge_cycle_indicator'] = False
        removed_capacity_check_idx = list(set(capacity_check_idx_0).symmetric_difference(set(charge_start_idx)))
        cell_data.loc[removed_capacity_check_idx,'capacity_check_indicator'] = False
    cell_data['cycle_indicator'] = cell_data.charge_cycle_indicator #default cycle indicator on charge

    # save cycle metrics to separate dataframe and sort. only keep columns where charge and discharge cycles start. Label the type of protocol
    cycle_metrics_columns = ['Time [s]','Ah throughput [A.h]', 'Test type','Protocol','discharge_cycle_indicator','cycle_indicator','charge_cycle_indicator','capacity_check_indicator', 'Test name']
    cell_cycle_metrics = cell_data[cycle_metrics_columns][(cell_data.discharge_cycle_indicator==True) | (cell_data.charge_cycle_indicator==True)].copy()
    # cell_cycle_metrics.sort_values(by=['Time [s]'])
    cell_cycle_metrics.reset_index(drop=True, inplace=True)
    return cell_data, cell_cycle_metrics