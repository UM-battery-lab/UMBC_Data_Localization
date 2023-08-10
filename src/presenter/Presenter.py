import datetime
from src.model.DataManager import DataManager

from src.logger_config import setup_logger

class Presenter:
    def __init__(self, dataManager: DataManager):
        self.dataManager = dataManager
        self.logger = setup_logger()

    def timestamp_to_datetime(self, t):
        t = t/1000
        tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=72000))
        return datetime.datetime.fromtimestamp(t, tz=tzinfo)

    def get_measured_data_time(self, cell_name, plot_cycles = True): 
        """
        Get measured data from the local disk

        Parameters
        ----------
        cell_name: str
            The cell name of the data to be found
        plot_cycles: bool, optional
            Whether to plot the cycles
        
        Returns
        -------
        dict
            The dictionary of measured data
        """
        # setup measured data
        cell_cycle_metrics, cell_data, cell_data_vdf = self.dataManager.process_cell(cell_name)
        self.logger.info(f'cell_data: {cell_data}')
        # setup timeseries data
        t = cell_data['Time [s]'].apply(self.timestamp_to_datetime)
        I = cell_data['Current [A]']
        V = cell_data['Voltage [V]'] 
        T = cell_data['Temperature [degC]'] 
        AhT = cell_data['Ah throughput [A.h]']
        t_vdf = cell_data_vdf['Time [s]'].apply(self.timestamp_to_datetime)
        exp_vdf = cell_data_vdf['Expansion [-]']
        T_vdf = cell_data_vdf['Temperature [degC]']
        # T_amb = cell_data_vdf['Amb Temp [degC]']    
        # setup cycle metrics
        t_cycle = cell_cycle_metrics['Time [s]'].apply(self.timestamp_to_datetime) 
        Q_c = cell_cycle_metrics['Charge capacity [A.h]'] 
        Q_d = cell_cycle_metrics['Discharge capacity [A.h]'] 

        cycle_idx = []
        capacity_check_idx = []
        cycle_idx_vdf = []
        capacity_check_in_cycle_idx = []
        charge_idx = []

        if plot_cycles:
            cycle_idx = cell_data.cycle_indicator[cell_data.cycle_indicator].index     # indices in cell_data to check cycle alignment
            capacity_check_idx = cell_data.capacity_check_indicator[cell_data.capacity_check_indicator].index 
            cycle_idx_vdf = cell_data_vdf.cycle_indicator[cell_data_vdf.cycle_indicator].index
            capacity_check_in_cycle_idx = cell_cycle_metrics[cell_cycle_metrics.capacity_check_indicator].index
            charge_idx = cell_data.charge_cycle_indicator[cell_data.charge_cycle_indicator].index
            step_idx = cell_data['Step index']

        self.logger.info(f'Get {len(t)} data points.')
        return {
            't': t,
            'I': I,
            'V': V,
            'T': T,
            'AhT': AhT,
            't_vdf': t_vdf,
            'exp_vdf': exp_vdf,
            'T_vdf': T_vdf,
            't_cycle': t_cycle,
            'Q_c': Q_c,
            'Q_d': Q_d,
            'cycle_idx': cycle_idx,
            'capacity_check_idx': capacity_check_idx,
            'cycle_idx_vdf': cycle_idx_vdf,
            'capacity_check_in_cycle_idx': capacity_check_in_cycle_idx,
            'charge_idx': charge_idx,
        }
    