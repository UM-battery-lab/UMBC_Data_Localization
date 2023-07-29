import numpy as np
import pandas as pd



def filter_test_record_by_name(name, tag, device_trs): 
    """
    Filter the test records by name and tag

    Parameters
    ----------
    name: str
        The name of the test records to be found
    tag: str
        The tag of the test records to be found
    device_trs: list of TestRecord objects
        The list of test records to be searched
    
    Returns
    -------
    list of TestRecord objects
        The list of test records that match the specified name and tag
    """
    if tag !='':
        tests = [tr for tr in device_trs if name.lower() in tr.name.lower()  and tag.lower() in tr.tags ] #and (t.status =='DONE' or '_F'.lower() or 'REF'.lower() in t.name.lower()) 
    else:
        tests = [tr for tr in device_trs if name.lower() in tr.name.lower()]
    return tests

def get_device_test_records(device, trs):
    """
    Get the test records of the specified device

    Parameters
    ----------
    device: Device object
        The device whose test records are to be found
    trs: list of TestRecord objects
        The list of test records to be searched
    
    Returns
    -------
    list of TestRecord objects
        The list of test records of the specified device
    """
    device_id = device.id
    return [tr for tr in trs if device_id == tr.device_id]

def filter_devices_by_name(devs, name):
    """
    Filter the devices by name

    Parameters
    ----------
    devs: list of Device objects
        The list of devices to be searched
    name: str
        The name of the devices to be found
    
    Returns
    -------
    list of Device objects
        The list of devices that match the specified name
    """
    return [dev for dev in devs if name.lower() in dev.name.lower()]

def get_tests(Device, trs, test_type='', tag=''): 
    """
    Get the test records of the specified device matching the specified test type and tag

    Parameters
    ----------
    Device: Device object
        The device whose test records are to be found
    trs: list of TestRecord objects
        The list of test records to be searched
    test_type: str, optional
        The type of the test records to be found
    tag: str, optional
        The tag of the test records to be found

    Returns
    -------
    list of TestRecord objects
        The list of test records of the specified device matching the specified test type and tag
    """
    device_trs=get_device_test_records(Device, trs)
    filtered_trs = filter_test_record_by_name(test_type,tag,device_trs)

    return filtered_trs

def sort_tests(trs):
    """
    Sort the test records by start time

    Parameters
    ----------
    trs: list of TestRecord objects
        The list of test records to be sorted
    
    Returns
    -------
    list of TestRecord objects
        The list of test records sorted by start time 
    """
    idx_sorted = np.argsort([test.start_time for test in trs])
    trs_sorted = [trs[i] for i in idx_sorted]
    return trs_sorted

def filter_trs_new_data(cell_cycle_metrics, trs, last_cycle_time = []):
    """
    Get the list of test records that have not been processed

    Parameters
    ----------
    cell_cycle_metrics: DataFrame
        The dataframe of the cell cycle metrics
    trs: list of TestRecord objects
        The list of test records to be filtered
    last_cycle_time: float, optional
        The timestamp of the last cycle

    Returns
    -------
    list of TestRecord objects
        The list of test records that have not been processed
    """
    recorded_cycle_times = cell_cycle_metrics['Time [s]']
    #TODO: check if this is the right way to do this
    load_new_data = [True for i in range(len(trs))] #init
    trs_new_data = []
    # for each file, check that cell_cycle_metrics has timestamps in this range
    for test in trs:
        cycle_end_times_raw = test.get_cycle_stats().cyc_end_datapoint_time #from cycler's cycle count
        cycle_end_times = pd.to_datetime(cycle_end_times_raw, unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        if len(last_cycle_time) ==0: # if a timestamp isn't passed in
            last_cycle_time_in_file = cycle_end_times.iloc[-1]
        else:
            last_cycle_time_in_file  = last_cycle_time
        if len(cycle_end_times) > 1: #ignore aux data and files with partial cycle. does this still work for vdf?
            timestamps_in_range = [True for t in recorded_cycle_times if test.start_time <= t and t <=last_cycle_time_in_file]
            if len(timestamps_in_range)==0:
                trs_new_data.append(test) 
    return trs_new_data
