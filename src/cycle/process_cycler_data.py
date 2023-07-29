import numpy as np
from cycle.combine_cycler_data import combine_cycler_data
from cycle.utils import max_min_cycle_data, calc_capacities


def process_cycler_data(trs_neware, cycle_id_lims, numFiles=1000, print_filenames = False):
    """
    Process cycler data from a list of test records

    Parameters
    ----------
    trs_neware: list of TestRecord objects
        The list of test records to be processed
    cycle_id_lims: list of ints
        The cycle number limits for charge, discharge, and total cycles
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
    """


    # combine data for all files 
    cell_data, cell_cycle_metrics = combine_cycler_data(trs_neware, cycle_id_lims, numFiles = numFiles, print_filenames = print_filenames)
    
    # calculate capacities 
    charge_t_idx = list(cell_data[cell_data.charge_cycle_indicator ==True].index)
    discharge_t_idx = list(cell_data[cell_data.discharge_cycle_indicator ==True].index)
    Q_c, Q_d = calc_capacities(cell_data['Time [s]'], cell_data['Current [A]'], cell_data['Ah throughput [A.h]'], charge_t_idx, discharge_t_idx)
    
    # Find min/max metrics
    cycle_idx_minmax = list(cell_data[cell_data.cycle_indicator ==True].index)
    cycle_idx_minmax.append(len(cell_data)-1)
    V_max, V_min = max_min_cycle_data(cell_data['Voltage [V]'], cycle_idx_minmax)
    T_max, T_min = max_min_cycle_data(cell_data['Temperature [degC]'], cycle_idx_minmax)

    cell_cycle_metrics['Charge capacity [A.h]'] = [np.nan]*len(cell_cycle_metrics) # init capacity columns in cell_cycle_metrics
    cell_cycle_metrics['Discharge capacity [A.h]'] = [np.nan]*len(cell_cycle_metrics)
    cell_cycle_metrics['Min cycle voltage [V]'] = [np.nan]*len(cell_cycle_metrics) # init capacity columns in cell_cycle_metrics
    cell_cycle_metrics['Max cycle voltage [V]'] = [np.nan]*len(cell_cycle_metrics)
    cell_cycle_metrics['Min cycle temperature [degC]'] = [np.nan]*len(cell_cycle_metrics) # init capacity columns in cell_cycle_metrics
    cell_cycle_metrics['Max cycle temperature [degC]'] = [np.nan]*len(cell_cycle_metrics)
    
    # Add to dataframe
    charge_cycle_number = list(cell_cycle_metrics[cell_cycle_metrics.charge_cycle_indicator ==True].index) # aligns with charge start
    discharge_cycle_number = list(cell_cycle_metrics[cell_cycle_metrics.discharge_cycle_indicator ==True].index) # aligns with discharge start
    cycle_number = list(cell_cycle_metrics[cell_cycle_metrics.cycle_indicator ==True].index) # align with charge start
    for i,j in enumerate(charge_cycle_number): 
        cell_cycle_metrics.loc[j, 'Charge capacity [A.h]'] = Q_c[i] 
    for i,j in enumerate(discharge_cycle_number): 
        cell_cycle_metrics.loc[j, 'Discharge capacity [A.h]'] = Q_d[i] 
    for i,j in enumerate(cycle_number): 
        cell_cycle_metrics.loc[j, 'Min cycle voltage [V]'] = V_min[i] 
        cell_cycle_metrics.loc[j, 'Max cycle voltage [V]'] = V_max[i] 
        cell_cycle_metrics.loc[j, 'Min cycle temperature [degC]'] = T_min[i] 
        cell_cycle_metrics.loc[j, 'Max cycle temperature [degC]'] = T_max[i] 
                
    return cell_data, cell_cycle_metrics