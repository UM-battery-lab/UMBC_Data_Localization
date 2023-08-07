import pandas as pd 
import numpy as np
import time
from scipy import integrate
from scipy.signal import find_peaks, medfilt
from itertools import compress

from src.model.DirStructure import DirStructure
from src.model.DataFilter import DataFilter
from src.logger_config import setup_logger
from src.constants import CYCLE_ID_LIMS


class DataProcessor:
    """
    The class to process the data

    Attributes
    ----------
    dataIO: DataIO object
        The object to save and load data
    dataFilter: DataFilter object
        The object to filter data from the local disk
    dirStructure: DirStructure object
        The object to manage the directory structure for the local data
    logger: logger object
        The object to log information
    
    
    Methods
    -------
    process_cell(trs_neware, trs_cycler, trs_vdf, cell_cycle_metrics=None, cell_data=None, cell_data_vdf=None, numFiles=1000, cycle_id_lims=CYCLE_ID_LIMS)
        Process the cell data from a list of test records
    process_cycler_data(trs, cycle_id_lims, numFiles = 1000)
        Process cycler data from a list of test records
    """
    def __init__(self, dataFilter: DataFilter, dirStructure: DirStructure):
        self.dataFilter = dataFilter
        self.dirStructure = dirStructure
        self.logger = setup_logger()

    def process_cell(self, trs_neware, trs_cycler, trs_vdf, cell_cycle_metrics=None, cell_data=None, cell_data_vdf=None, numFiles=1000, cycle_id_lims=CYCLE_ID_LIMS):
        """
        Process the cell data from a list of test records

        Parameters
        ----------
        trs_neware: list of TestRecord objects
            The list of test records from neware
        trs_cycler: list of TestRecord objects
            The list of test records from cycler
        trs_vdf: list of TestRecord objects 
            The list of test records from vdf
        cell_cycle_metrics: DataFrame, optional
            The dataframe of the cell cycle metrics
        cell_data: DataFrame, optional
            The dataframe of the cell data
        cell_data_vdf: DataFrame, optional
            The dataframe of the cell data vdf
        numFiles: int, optional
            The number of files to be processed
        cycle_id_lims: dict, optional
            The dictionary of the cycle id limits
        
        Returns
        -------
        DataFrame
            The dataframe of the cell cycle metrics
        DataFrame
            The dataframe of the cell data
        DataFrame
            The dataframe of the cell data vdf
        bool
            Whether the data needs to be updated
        """

        # 1. check if cell_data and cell_cycle_metrics pickle files exist. If so, add new test data to cell_data and cell_cycle_metrics
        load_new_data = [True for i in range(len(trs_neware))] # Initialization: indicates if the files were processed previously 
        if cell_cycle_metrics is None and cell_data is not None:
            # Make list of data files with new data to process
            trs_new_data = self.__filter_trs_new_data(cell_cycle_metrics, trs_cycler)
            # For each new file, load the data and add it to the existing dfs
            for test in trs_new_data: 
                self.logger.info(f"Processing {test.name}")
                # process test file
                cell_data_new, cell_cycle_metrics_new = self.process_cycler_data([test], cycle_id_lims=cycle_id_lims, numFiles = numFiles)

                # load test data to df and get start and end times
                df_test = self.__test_to_df(test, test_trace_keys = ['aux_vdf_timestamp_datetime_0'], df_labels =['Time [s]'])
                file_start_time = df_test['Time [s]'].iloc[0]
                file_end_time = df_test['Time [s]'].iloc[-1] 

                # insert new timeseries test data into cell_data and recalculate AhT
                cell_data = self.__update_dataframe(cell_data,cell_data_new, file_start_time, file_end_time)
                t = cell_data['Time [s]']
                I = cell_data['Current [A]']

                # insert new cycle metrics from test to cell_cycle_metrics  
                cell_cycle_metrics = self.__update_dataframe(cell_cycle_metrics,cell_cycle_metrics_new, file_start_time, file_end_time)
                cell_cycle_metrics['Ah throughput [A.h]'] = cell_data['Ah throughput [A.h]'][(cell_data.discharge_cycle_indicator==True) | (cell_data.charge_cycle_indicator==True)]
        
        else: # if pickle file doesn't exist, process all cycling data
            trs_new_data = trs_cycler.copy()
            cell_data, cell_cycle_metrics = self.process_cycler_data(trs_new_data, cycle_id_lims=cycle_id_lims, numFiles = numFiles)
        
        # 2. check if cell_data_vdf and cell_cycle_metrics pickle files exist. If so, list new test vdf data, else list all test vdf files.
        load_new_data_vdf = [True for i in range(len(trs_vdf))] # Initialization: indicates if the files were processed previously 
        if len(trs_vdf)==0: # make empty dfs for constrained cells 
            cell_data_vdf = pd.DataFrame(columns=['Time [s]','Expansion [-]', 'Expansion ref [-]', 'Temperature [degC]','cycle_indicator'])
            cell_cycle_metrics['Max cycle expansion [-]'] = np.nan
            cell_cycle_metrics['Min cycle expansion [-]'] = np.nan
            cell_cycle_metrics['Reversible cycle expansion [-]'] = np.nan
            trs_new_data_vdf = []
        elif cell_data_vdf is not None:
            # Make list of data files with new data to process
            trs_new_data_vdf = self.__filter_trs_new_data(cell_cycle_metrics, trs_vdf)
            if len(trs_new_data_vdf)>0: #ignore for constrained cells
                for test in trs_new_data_vdf:
                    self.logger.info(f"Processing {test.name}")
                    # process test file
                    cell_data_vdf_new, cell_cycle_metrics_new = self.__process_cycler_expansion([test], cell_cycle_metrics_new,numFiles = numFiles)

                    # load test data to df and get start and end times
                    df_test = self.__test_to_df(test, test_trace_keys = ['h_datapoint_time'], df_labels =['Time [s]'])
                    file_start_time = df_test['Time [s]'].iloc[0] 
                    file_end_time = df_test['Time [s]'].iloc[-1] 

                    # insert new timeseries test data into cell_data and cell_cycle_metrics
                    cell_data_vdf = self.__update_dataframe(cell_data_vdf,cell_data_vdf_new, file_start_time, file_end_time, update_AhT = False)
                    cell_cycle_metrics = self.__update_dataframe(cell_data,cell_cycle_metrics_new, file_start_time, file_end_time, update_AhT = False)

        else: # if pickle file doesn't exist or load_pickle is False, (re)process all expansion data
            trs_new_data_vdf = trs_vdf.copy()
            cell_data_vdf, cell_cycle_metrics = self.__process_cycler_expansion(trs_vdf, cell_cycle_metrics, numFiles = numFiles)    

        # rearrange columns of cell_cycle_metrics for easy reading with data on left and others on right
        cols = cell_cycle_metrics.columns.to_list()
        move_idx = [c for c in cols if '[' in c] + [c for c in cols if '[' not in c] # Columns with data include '[' in the key
        cell_cycle_metrics = cell_cycle_metrics[move_idx]

        # if there is new data, save it to pickle files
        update = len(trs_new_data)>0

        return cell_cycle_metrics, cell_data, cell_data_vdf, update
    
    def sort_tests(self, trs):
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
    
    def __filter_trs_new_data(self, cell_cycle_metrics, trs, last_cycle_time = []):
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
    
    def __update_dataframe(self, df, df_new, file_start_time, file_end_time, update_AhT = True): 
        """
        Update the dataframe with the new test data, and update the Ah throughput.

        Parameters
        ----------
        df: DataFrame
            The dataframe to be updated
        df_test: DataFrame
            The dataframe of the new test data
        file_start_time: float
            The start time of the new test data, get from df['Time [s]'].iloc[0]
        file_end_time: float
            The end time of the new test data, get from df['Time [s]'].iloc[-1]
        update_AhT: bool, optional
            Whether to update the Ah throughput

        Returns
        -------
        DataFrame
            The updated dataframe
        """

        # drop rows that have timestamps between the test start and end times to avoid overlapping time
        file_drop_idx = df[(df['Time [s]'] >= file_start_time) & (df['Time [s]'] <= file_end_time)].index 
        df.drop(file_drop_idx, inplace = True)

        # split old dataframe into data into before df_test and after df_test. Then reconcatonate with df_test in between. Update AhT.  
        if len(file_drop_idx) > 0: # add data from running test: replace partial existing file data
            df_before_test = df.iloc[0:file_drop_idx[0]-1].copy()
            df_after_test = df.iloc[file_drop_idx[-1]+1::].copy()
            
            # update AhT assuming field exists
            if update_AhT: 
                last_AhT_before_test = df['Ah throughput [A.h]'].iloc[file_drop_idx[0]-1]
                df_new['Ah throughput [A.h]'] = df_new['Ah throughput [A.h]'] + last_AhT_before_test
                last_AhT_from_test = df_new['Ah throughput [A.h]'].iloc[-1]
                df['Ah throughput [A.h]'] = df['Ah throughput [A.h]'] + last_AhT_from_test
            df = pd.concat([df_before_test,df_new,df_after_test])
            df.reset_index(drop=True, inplace=True)
        else: # add data from new test to the end of existing df 
            if update_AhT: 
                last_AhT_before_test = df['Ah throughput [A.h]'].iloc[-1]
                df_new['Ah throughput [A.h]'] = df_new['Ah throughput [A.h]'] + last_AhT_before_test
            df = pd.concat([df, df_new])
        
        # reset df index
        df.reset_index(drop=True, inplace=True) 
    
        return df

    def summarize_rpt_data(self, cell_data, cell_data_vdf, cell_cycle_metrics):
        """
        Get the summary data for each RPT file

        Parameters
        ----------
        cell_data: DataFrame
            The dataframe of the cell data
        cell_data_vdf: DataFrame
            The dataframe of the cell data vdf
        cell_cycle_metrics: DataFrame
            The dataframe of the cell cycle metrics
        
            
        Returns
        -------
        DataFrame
            The dataframe of the summary data for each RPT file
        """
        rpt_filenames = list(set(cell_cycle_metrics['Test name'][(cell_cycle_metrics['Test type'] == 'RPT') | (cell_cycle_metrics['Test type'] == '_F')]))
        cycle_summary_cols = [c for c in cell_cycle_metrics.columns.to_list() if '[' in c] + ['Test name', 'Protocol']
        cell_rpt_data = pd.DataFrame() 
        
        # for each RPT file (not sure what it'll do if there are multiple RPT files for 1 RPT...)
        for j,rpt_file in enumerate(rpt_filenames):
            rpt_idx = cell_cycle_metrics[cell_cycle_metrics['Test name'] == rpt_file].index

            # for each section of the RPT...
            for i in rpt_idx:
                rpt_subcycle = pd.DataFrame()
                #find timestamps for partial cycle
                t_start = cell_cycle_metrics['Time [s]'].loc[i]
                try: # end of partial cycle = next time listed
                    t_end = cell_cycle_metrics['Time [s]'].loc[i+1]
                except: # end of partial cycle = end of file
                    t_end = cell_data['Time [s]'].iloc[-1]

                # log summary stats for this partial cycle in dictionary
                rpt_subcycle['RPT #'] = j
                rpt_subcycle = cell_cycle_metrics[cycle_summary_cols].loc[i].to_dict()

                # add cycler data to dictionary
                t = cell_data['Time [s]']
                rpt_subcycle['Data'] = [cell_data[['Time [s]', 'Current [A]', 'Voltage [V]', 'Ah throughput [A.h]', 'Temperature [degC]', 'Step index']][(t>t_start) & (t<t_end)]]
                
                # add vdf data to dictionary
                t_vdf = cell_data_vdf['Time [s]']
                if len(t_vdf)>1: #ignore for constrained cells
                    rpt_subcycle['Data vdf'] = [cell_data_vdf[(t_vdf>t_start) & (t_vdf<t_end)]]

                # convert and add dictionary to dataframe
                cell_rpt_data= pd.concat([cell_rpt_data, pd.DataFrame.from_dict(rpt_subcycle)])
        # format df: put protocol in front and reindex
        cell_rpt_data.reset_index(drop=True, inplace=True)
        cols = cell_rpt_data.columns.to_list()
        if cols != []:
            cell_rpt_data = cell_rpt_data[[cols[len(cols)-1]] + cols[0:-1]] 

        return cell_rpt_data

    def __process_cycler_expansion(self, trs_vdf, cell_cycle_metrics, numFiles = 1000, t_match_threshold=60):
        # Combine vdf data into a single df
        cell_data_vdf = self.__combine_cycler_expansion(trs_vdf, numFiles)
        
        # Find matching cycle timestamps from cycler data
        t_vdf = cell_data_vdf['Time [s]']
        exp_vdf = cell_data_vdf['Expansion [-]']
        cycle_timestamps = cell_cycle_metrics['Time [s]'][cell_cycle_metrics.cycle_indicator==True]
        t_cycle_vdf, cycle_idx_vdf, matched_timestamp_indices = self.__find_matching_timestamp(cycle_timestamps, t_vdf, t_match_threshold=10)  

        # add cycle indicator. These should align with cycles timestamps previously defined by cycler data
        cell_data_vdf['cycle_indicator'] = list(map(lambda x: x in cycle_idx_vdf, range(len(cell_data_vdf))))
        
        # find min/max expansion
        cycle_idx_vdf_minmax = [i for i in cycle_idx_vdf if i is not np.nan]
        cycle_idx_vdf_minmax.append(len(t_vdf)-1) #append end
        exp_max, exp_min = self.__max_min_cycle_data(exp_vdf, cycle_idx_vdf_minmax)
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
        t_charge_cycle_vdf, charge_cycle_idx_vdf, matched_charge_timestamp_indices = self.__find_matching_timestamp(charge_cycle_timestamps, t_vdf, t_match_threshold=10)
        for i,j in enumerate(matched_charge_timestamp_indices):
            cell_cycle_metrics.loc[charge_cycle_idx[j], 'Time vdf [s]'] = t_charge_cycle_vdf[i]

        return cell_data_vdf, cell_cycle_metrics


    def __combine_cycler_expansion(self, trs_vdf, numFiles = 1000):
        """
        PROCESS NEWARE VDF DATA
        Reads in data from the last "numFiles" files in the "trs_vdf" and concatenates them into a long dataframe. Then looks for corresponding cycle start/end timestamps in vdf time. 
        Finally, it'll calculate the min, max, and reversible expansion for each cycle.  
        """
        # concatenate vdf data frames for last numFiles files
        frames_vdf =[]
        # For each vdf file...
        for test_vdf in trs_vdf[0:min(len(trs_vdf), numFiles)]:
            try:
                # Read in timeseries data from test and formating into dataframe. Remove rows with expansion value outliers.
                self.logger.info(f"Now Processing {test_vdf.name}")
                # df_vdf = test2df(test_vdf, test_trace_keys = ['aux_vdf_timestamp_datetime_0','aux_vdf_ldcsensor_none_0', 'aux_vdf_ldcref_none_0', 'aux_vdf_ambienttemperature_celsius_0', 'aux_vdf_temperature_celsius_0'], df_labels =['Time [s]','Expansion [-]', 'Expansion ref [-]', 'Amb Temp [degC]', 'Temperature [degC]'])
                df_vdf = self.__test_to_df(test_vdf, test_trace_keys = ['aux_vdf_timestamp_datetime_0','aux_vdf_ldcsensor_none_0', 'aux_vdf_ldcref_none_0', 'aux_vdf_ambienttemperature_celsius_0'], df_labels =['Time [s]','Expansion [-]', 'Expansion ref [-]','Temperature [degC]'])
                df_vdf = df_vdf[(df_vdf['Expansion [-]'] >1e1) & (df_vdf['Expansion [-]'] <1e7)] #keep good signals 
                df_vdf['Temperature [degC]'] = np.where((df_vdf['Temperature [degC]'] >= 200) & (df_vdf['Temperature [degC]'] <250), np.nan, df_vdf['Temperature [degC]']) 
                # df_vdf['Amb Temp [degC]'] = np.where((df_vdf['Amb Temp [degC]'] >= 200) & (df_vdf['Amb Temp [degC]'] <250), np.nan, df_vdf['Amb Temp [degC]']) 
                frames_vdf.append(df_vdf)
                self.logger.info(f"Finished processing with {len(frames_vdf)} data points")
            except: #Tables are different Length, cannot merge
                pass
            time.sleep(0.1) 
        
        if (len(frames_vdf) == 0):
            return pd.DataFrame(columns=['Time [s]','Expansion [-]', 'Expansion ref [-]', 'Temperature [degC]','cycle_indicator'])
        # Combine vdf data into a single df and reset the index 
        cell_data_vdf = pd.concat(frames_vdf).sort_values(by=['Time [s]'])
        cell_data_vdf.reset_index(drop=True, inplace=True)

        return cell_data_vdf

    def process_cycler_data(self, trs_neware, cycle_id_lims, numFiles=1000):
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

        Returns
        -------
        DataFrame
            The processed data
        DataFrame
            The processed cycle metrics
        """


        # combine data for all files 
        cell_data, cell_cycle_metrics = self.__combine_cycler_data(trs_neware, cycle_id_lims, numFiles = numFiles)
        
        # calculate capacities 
        charge_t_idx = list(cell_data[cell_data.charge_cycle_indicator ==True].index)
        discharge_t_idx = list(cell_data[cell_data.discharge_cycle_indicator ==True].index)
        Q_c, Q_d = self.__calc_capacities(cell_data['Time [s]'], cell_data['Current [A]'], cell_data['Ah throughput [A.h]'], charge_t_idx, discharge_t_idx)
        
        # Find min/max metrics
        cycle_idx_minmax = list(cell_data[cell_data.cycle_indicator ==True].index)
        cycle_idx_minmax.append(len(cell_data)-1)
        V_max, V_min = self.__max_min_cycle_data(cell_data['Voltage [V]'], cycle_idx_minmax)
        T_max, T_min = self.__max_min_cycle_data(cell_data['Temperature [degC]'], cycle_idx_minmax)

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
        
    def __max_min_cycle_data(self, data, cycle_idx_minmax):
        """
        Get the max and min data for each cycle

        Parameters
        ----------
        data: list of floats
            The data to be processed
        cycle_idx_minmax: list of ints
            The list of cycle indices

        Returns
        -------
        list of floats
            The max data for each cycle
        list of floats
            The min data for each cycle
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

    def __calc_capacities(self, t, I, AhT, charge_idx, discharge_idx):
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

    def __combine_cycler_data(self, trs_cycler, cycle_id_lims, numFiles=1000, last_AhT = 0, debug = True):
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
                    test_data = self.__test_to_df(test,test_trace_keys_arbin, df_labels_arbin, ms = isRPT)
                    test_data['Temperature [degC]'] = [np.nan]*len(test_data) # make arbin tables with same columns as neware files
                # 1b. for neware files
                elif 'neware_xls_4000' in test.tags: 
                    test_data = self.__test_to_df(test, ms = isRPT)
                    test_data['Temperature [degC]'] = np.where((test_data['Temperature [degC]'] >= 200) & (test_data['Temperature [degC]'] <250), np.nan, test_data['Temperature [degC]']) 
                
                self.logger.info(f"Get {len(test_data)} rows of data from {test.name}")
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
                    charge_start_idx_file, discharge_start_idx_file = self.__match_charge_discharge(np.array(charge_start_idx_file), np.array(discharge_start_idx_file))
                else: # find I==0 and filter out irrelevant points
                    charge_start_idx_file, discharge_start_idx_file = self.__find_cycle_idx(t, I, V, AhT, step_idx, V_max_cycle = V_max_cycle, V_min_cycle = V_min_cycle, dt_min = dt_min, dAh_min= dAh_min)
                    try: # won't work for half cycles (files with only charge or only discharge)
                        charge_start_idx_file, discharge_start_idx_file = self.__match_charge_discharge(charge_start_idx_file, discharge_start_idx_file)
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
                if debug: # for debugging
                    self.logger.info(test.name + '   Cycles: ' + str(len(charge_start_idx_file)) + '   AhT: ' + str(round(AhT.iloc[-1],2)))
                
                frames.append(test_data)
        
            except Exception as e: # Error: Tables are Different Length, cannot merge or corrupted files
                self.logger.error('\033[91m'+test.name + ' (' + str(e) + ' )' + '\033[0m')

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
            charge_start_idx, discharge_start_idx = self.__match_charge_discharge(charge_start_idx_0, discharge_start_idx_0) 
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

    def __test_to_df(self, tr, test_trace_keys = ['h_datapoint_time','h_test_time','h_current', 'h_potential', 'c_cumulative_capacity', 'aux_neware_xls_t1_none_0','h_step_index'], df_labels = ['Time [s]','Test Time [s]', 'Current [A]', 'Voltage [V]', 'Ah throughput [A.h]', 'Temperature [degC]', 'Step index'], ms = False):
        """
        Filter and format data from a TestRecord object into a dataframe

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
        df_raw = self.dataFilter.filter_df_by_tr(tr, trace_keys = test_trace_keys)
        
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
        df = df_raw.set_axis(df_labels, axis=1)
        return df

    def __find_cycle_idx(self, t, I, V, AhT, step_idx, V_max_cycle=3, V_min_cycle=4, dt_min = 600, dAh_min=1):
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
        charge_start_idx, discharge_start_idx = self.__filter_cycle_idx(current_sign_change_idx, t, I, V, AhT, V_max_cycle=V_max_cycle, V_min_cycle=V_min_cycle, dt_min = dt_min, dAh_min = dAh_min)
        return charge_start_idx, discharge_start_idx

    def __filter_cycle_idx(self, cycle_idx0, t, I, V, AhT, V_max_cycle=3, V_min_cycle=4, dt_min = 600, dAh_min=1):
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

    def __match_charge_discharge(self, charge_start_idx_0, discharge_start_idx_0):
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

    def __find_matching_timestamp(self, desired_timestamps, t, t_match_threshold=60, nan_pad = False):
        """
        Find the matching timestamps

        Parameters
        ----------
        desired_timestamps: list of floats
            The list of desired timestamps
        t: floats
            The time data
        t_match_threshold: float, optional
            The threshold for matching timestamps
        nan_pad: bool, optional
            Whether to pad with nan
        
        Returns
        -------
        list of floats
            The list of matching timestamps
        list of ints
            The list of mapped indices
        list of ints
            The list of matched timestamp indices
        """

        # find indices for "desired_timestamps" in an array of timestamps "t" within "t_match_threshold" seconds  
        mapped_indices = [] #indexes t
        matched_timestamp_indices =[] # indexes desired_timestamps
        matched_timestamps = [] # value of t closest to desired_timestamp. includes nan if can't find matching timestamp.
        # for each timestamp...
        for k,desired_timestamp in enumerate(desired_timestamps):
            # if smallest dt < t_match_threshold
            if np.min(abs(t-desired_timestamp)).total_seconds()<t_match_threshold:
                # save index in t of nearest value of t and corresponding t
                matched_idx = np.argmin(abs(t-desired_timestamp))
                mapped_indices.append(matched_idx)
                matched_timestamps.append(t[matched_idx])
                
                # save index in desired_timestamps. used to match cycle number
                matched_timestamp_indices.append(k)
            
            elif nan_pad: # else if pad with nan if requested
                matched_timestamps.append(np.nan)

        return matched_timestamps, mapped_indices, matched_timestamp_indices