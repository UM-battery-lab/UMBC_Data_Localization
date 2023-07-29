import numpy as np
import pandas as pd

from fetch.fetch_data import get_data_from_test_record

def test2df(tr, test_trace_keys = ['h_datapoint_time','h_test_time','h_current', 'h_potential', 'c_cumulative_capacity', 'aux_neware_xls_t1_none_0','h_step_index'], df_labels = ['Time [s]','Test Time [s]', 'Current [A]', 'Voltage [V]', 'Ah throughput [A.h]', 'Temperature [degC]', 'Step index']):
    """
    Get DataFrame from test record

    Parameters
    ----------
    tr: TestRecord object
        The test record to be processed
    test_trace_keys: list of str, optional
        The list of test trace keys to be extracted
    df_labels: list of str, optional
        The list of labels for the dataframe keys

    Returns
    -------
    DataFrame
        The processed dataframes
    """
    
    # Read in timeseries data from test and formating into dataframe
    df_raw = get_data_from_test_record(tr, trace_keys = test_trace_keys)
    
    # convert timestamps to test time
    if 'h_datapoint_time' in test_trace_keys: 
    #TODO: fix this hack to bring back fractional seconds during the pulse tests. This may break if you pause and resume a test?    
    #     if ms:  
    #         df_raw['h_datapoint_time'] = pd.to_datetime(df_raw['h_datapoint_time'][0]+df_raw['h_test_time']*1000, unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    #     else: 
        df_raw['h_datapoint_time'] = pd.to_datetime(df_raw['h_datapoint_time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

    if 'aux_vdf_timestamp_datetime_0' in test_trace_keys:
        df_raw['aux_vdf_timestamp_datetime_0'] = pd.to_datetime(df_raw['aux_vdf_timestamp_datetime_0'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

    # preserve listed trace key order and rename columns for easy calling
    df_raw = df_raw[test_trace_keys]
    df = df_raw.set_axis(df_labels, axis=1, inplace=False)

    return df


def max_min_cycle_data(data, cycle_idx_minmax):
    """
    Get the max and min data for each cycle

    Parameters
    ----------
    data: list of floats
        The data to be processed
    cycle_idx_minmax: list of ints
        The list of cycle indices
    """

    # calculate min and max data for each cycle (e.g. voltage, temperature, or expansion)
    y_max  = []
    y_min  = []
    # for each cycle...
    for i in range(len(cycle_idx_minmax)-1):
        # if there's cycle data between two consecutive points...
        if len(data[cycle_idx_minmax[i]:cycle_idx_minmax[i+1]])>0:
            y_max.append(max(data[cycle_idx_minmax[i]:cycle_idx_minmax[i+1]]))
            y_min.append(min(data[cycle_idx_minmax[i]:cycle_idx_minmax[i+1]]))
        # handling edge cases
        else: 
            y_max.append(data[cycle_idx_minmax[i]])   
            y_min.append(data[cycle_idx_minmax[i]])  
    return y_max, y_min

def calc_capacities(t, I, AhT, charge_idx, discharge_idx):
    """
    Calculate the charge and discharge capacities

    Parameters
    ----------
    t: list of floats
        The time data
    I: list of floats
        The current data
    AhT: list of floats
        The Ah throughput data
    charge_idx: list of ints
        The list of charge indices
    discharge_idx: list of ints
        The list of discharge indices

    Returns
    -------
    list of floats
        The charge capacities
    """
    # combine charge and discharge idx into a list. assumes there are the same length, and alternate charge-discharge (or vice versa)
    cycle_idx = charge_idx + discharge_idx
    cycle_idx.append(len(t)-1) # add last data point
    cycle_idx.sort() # should alternate charge and discharge start indices
    Q_c = []
    Q_d = []
    
    # Calculate capacity 
    for i in range(len(cycle_idx)-1):
        # Calculate capacity based on AhT.
        Q = AhT[cycle_idx[i+1]]-AhT[cycle_idx[i]]
        if cycle_idx[i] in charge_idx:
            Q_c.append(Q) 
        else:
            Q_d.append(Q) 
    return np.array(Q_c), np.array(Q_d)

def find_cycle_idx(t, I, V, AhT, step_idx, V_max_cycle=3, V_min_cycle=4, dt_min = 600, dAh_min=1):
    """
    Find the cycle charge and discharge indices by calling filter_cycle_idx

    Parameters
    ----------
    t: list of floats
        The time data
    I: list of floats
        The current data
    V: list of floats
        The voltage data
    AhT: list of floats
        The Ah throughput data
    step_idx: list of ints
        The step indices
    V_max_cycle: float, optional
        The maximum voltage for a cycle
    V_min_cycle: float, optional
        The minimum voltage for a cycle
    dt_min: float, optional
        The minimum time for a cycle
    dAh_min: float, optional
        The minimum Ah throughput for a cycle

    Returns
    -------
    list of ints
        The list of charge indices
    list of ints
        The list of discharge indices
    """
    # Find indices of sign changes and there's a change in step index and filter based on dt, dAh, and V
    current_sign_change_idx = np.where(np.diff(np.sign(I)).astype(bool) & (np.diff(step_idx) !=0))[0]
    current_sign_change_idx = np.sort(np.append(current_sign_change_idx,[AhT.first_valid_index(),len(t)-1])) # add the start and end for the diff checks
    
    # Filter to identify cycles based on threshold inputs
    charge_start_idx, discharge_start_idx = filter_cycle_idx(current_sign_change_idx, t, I, V, AhT, V_max_cycle=V_max_cycle, V_min_cycle=V_min_cycle, dt_min = dt_min, dAh_min = dAh_min)
    return charge_start_idx, discharge_start_idx

def filter_cycle_idx(cycle_idx0, t, I, V, AhT, V_max_cycle=3, V_min_cycle=4, dt_min = 600, dAh_min=1):
    """
    Filter the cycle indices based on the specified thresholds
    
    Parameters
    ----------
    cycle_idx0: list of ints
        The list of cycle indices
    t: list of floats
        The time data
    I: list of floats
        The current data
    V: list of floats
        The voltage data
    AhT: list of floats
        The Ah throughput data
    V_max_cycle: float, optional
        The maximum voltage for a cycle
    V_min_cycle: float, optional
        The minimum voltage for a cycle
    dt_min: float, optional
        The minimum time for a cycle
    dAh_min: float, optional
        The minimum Ah throughput for a cycle
    
    Returns
    -------
    list of ints
        The list of charge indices
    list of ints
        The list of discharge indices
    """
    dt_check_1 = [i for i,dt in enumerate(np.diff(t[cycle_idx0])) if dt.total_seconds() > dt_min]
    dt_check_1.append(len(cycle_idx0)-1) #add end of file
    dt_check = np.array(list(map(lambda x: x in dt_check_1, range(len(cycle_idx0)))))

    dAh_check_0 = [i for i,dAh in enumerate(np.diff(AhT[cycle_idx0])) if dAh > dAh_min]
    dAh_check_0.append(len(cycle_idx0)-1)  #add end of file
    dAh_check = np.array([True if i in dAh_check_0 else False for i in range(len(cycle_idx0))])
        
    # check that cycle start voltages are outside V(charge_start)<V_min and V(discharge_start)>V_max
    V_min_check = (V[cycle_idx0]<V_min_cycle).to_numpy()
    V_max_check = (V[cycle_idx0]>V_max_cycle).to_numpy()
    
    # combine checks 
    charge_start_idx = cycle_idx0[np.where(dt_check & dAh_check & V_min_check)[0]]
    discharge_start_idx = cycle_idx0[np.where(dt_check & dAh_check & V_max_check)[0]]
    return charge_start_idx, discharge_start_idx

def match_charge_discharge(charge_start_idx_0, discharge_start_idx_0):
    """
    Get the charge and discharge indices that match

    Parameters
    ----------
    charge_start_idx_0: list of ints
        The list of charge indices
    discharge_start_idx_0: list of ints
        The list of discharge indices
    
    Returns
    -------
    list of ints
        The list of charge indices that match
    list of ints
        The list of discharge indices that match
    """
    discharge_start_idx = []
    charge_start_idx = []
    # Filter out unnecessary cycle indices created when identifying cycles per filer. Length of charge and discharge start indices should be the same afterwards. 
    if discharge_start_idx_0[0]<charge_start_idx_0[0]: # if cycling starts on a discharge
        for i in range(len(charge_start_idx_0)):
            middle = charge_start_idx_0[i]
            left = discharge_start_idx_0[np.where(discharge_start_idx_0 < middle)[0][-1]]
            # right = discharge_start_idx_0[np.where(discharge_start_idx_0 > middle)[0][0]]
            discharge_start_idx.append(left)
            charge_start_idx.append(middle)
    else: # if cycling starts on a charge
        for i in range(len(discharge_start_idx_0)):
            middle = discharge_start_idx_0[i]
            left = charge_start_idx_0[np.where(charge_start_idx_0 < middle)[0][-1]]
            # right = charge_start_idx_0[np.where(charge_start_idx_0 > middle)[0][0]]
            charge_start_idx.append(left)
            discharge_start_idx.append(middle)
    return charge_start_idx, discharge_start_idx