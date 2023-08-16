import datetime
from src.model.DataManager import DataManager
from src.constants import DATE_FORMAT, TZ_INFO
from src.dto.DataTransferObject import TimeSeriesDTO, ExpansionDTO, CycleMetricsDTO, IndexMetricsDTO, CellDataDTO
from src.logger_config import setup_logger

#TODO: clean the structure of the code
class Presenter:
    def __init__(self, dataManager: DataManager):
        self.dataManager = dataManager
        self.logger = setup_logger()

    def timestamp_to_datetime(self, t):
        t = t/1000
        return datetime.datetime.fromtimestamp(t, tz=TZ_INFO)
    
    def str_to_timestamp(self, date_str):
        dt = datetime.datetime.strptime(date_str, DATE_FORMAT)
        dt = dt.replace(tzinfo=TZ_INFO)
        timestamp = dt.timestamp() * 1000
        return timestamp
    
    def _mask_data(self, data, start_time=None, end_time=None):
        if start_time:
            start_timestamp = self.str_to_timestamp(start_time)
            if not data.empty:
                mask = (data['Time [s]'] >= start_timestamp)
                data = data[mask]
        if end_time:
            end_timestamp = self.str_to_timestamp(end_time)
            if not data.empty:
                mask = (data['Time [s]'] <= end_timestamp)
                data = data[mask]
        return data
    
    def _extract_timeseries(self, data) -> TimeSeriesDTO:
        return TimeSeriesDTO(
            t=data['Time [s]'].apply(self.timestamp_to_datetime),
            I=data['Current [A]'],
            V=data['Voltage [V]'],
            T=data['Temperature [degC]'],
            AhT=data['Ah throughput [A.h]']
        )
    
    def _extract_expansion(self, data) -> ExpansionDTO:
        return ExpansionDTO(
            t_vdf=data['Time [s]'].apply(self.timestamp_to_datetime),
            exp_vdf=data['Expansion [-]'],
            T_vdf=data['Temperature [degC]']
        )
    
    def _extract_cycle_metrics(self, data, is_ccm=False) -> CycleMetricsDTO:
        if is_ccm:
            return CycleMetricsDTO(
                t_cycle=data['Time [s]'].apply(self.timestamp_to_datetime),
                Q_c=data['Charge capacity [A.h]'],
                Q_d=data['Discharge capacity [A.h]'],
                AhT_cycle=data['Ah throughput [A.h]'],
                V_min = data['Min cycle voltage [V]'],
                V_max = data['Max cycle voltage [V]'],
                T_min = data['Min cycle temperature [degC]'],
                T_max = data['Max cycle temperature [degC]'],
                exp_min = data['Max cycle expansion [-]'],
                exp_max = data['Min cycle expansion [-]'],
                exp_rev = data['Reversible cycle expansion [-]'],
            )     
        return CycleMetricsDTO(
            t_cycle=data['Time [s]'].apply(self.timestamp_to_datetime),
            Q_c=data['Charge capacity [A.h]'],
            Q_d=data['Discharge capacity [A.h]']
        )
    
    def _extract_index_metrics(self, cell_data, cell_data_vdf, cell_cycle_metrics, is_ah=False) -> IndexMetricsDTO:
        if is_ah:
            return IndexMetricsDTO(
                cycle_idx = cell_data.cycle_indicator[cell_data.cycle_indicator].index,     # indices in cell_data to check cycle alignment
                capacity_check_idx = cell_data.capacity_check_indicator[cell_data.capacity_check_indicator].index,
                capacity_check_in_cycle_idx = cell_cycle_metrics[cell_cycle_metrics.capacity_check_indicator].index,
            )
        return IndexMetricsDTO(
            cycle_idx = cell_data.cycle_indicator[cell_data.cycle_indicator].index,     # indices in cell_data to check cycle alignment
            capacity_check_idx = cell_data.capacity_check_indicator[cell_data.capacity_check_indicator].index,
            cycle_idx_vdf = cell_data_vdf.cycle_indicator[cell_data_vdf.cycle_indicator].index,
            capacity_check_in_cycle_idx = cell_cycle_metrics[cell_cycle_metrics.capacity_check_indicator].index,
            charge_idx = cell_data.charge_cycle_indicator[cell_data.charge_cycle_indicator].index
        )

    def _get_data(self, cell_name, start_time=None, end_time=None):
        # setup measured data
        cell_cycle_metrics, cell_data, cell_data_vdf, _ = self.dataManager.process_cell(cell_name)
        self.logger.debug(f'cell_data: {cell_data}')
        
        # Filter data based on start and end time
        cell_data = self._mask_data(cell_data, start_time, end_time)
        cell_data_vdf = self._mask_data(cell_data_vdf, start_time, end_time)
        cell_cycle_metrics = self._mask_data(cell_cycle_metrics, start_time, end_time)
        self.logger.info(f'Get measured data for cell {cell_name} from {start_time} to {end_time}')
        return cell_data, cell_data_vdf, cell_cycle_metrics

    def get_measured_data_time(self, cell_name, start_time=None, end_time=None, plot_cycles = True): 
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
        cell_data, cell_data_vdf, cell_cycle_metrics = self._get_data(cell_name, start_time, end_time)
        # setup dto
        timeseries = self._extract_timeseries(cell_data)
        expansion = self._extract_expansion(cell_data_vdf)
        cycle_metrics = self._extract_cycle_metrics(cell_cycle_metrics)
        index_metrics = self._extract_index_metrics(cell_data, cell_data_vdf, cell_cycle_metrics) if plot_cycles else None
        cell_data_dto = CellDataDTO(
            timeseries=timeseries,
            expansion=expansion,
            cycle_metrics=cycle_metrics,
            index_metrics=index_metrics 
        )
        return cell_data_dto

    def get_cycle_metrics_times(self, cell_name, start_time=None, end_time=None):
        cell_data, cell_data_vdf, cell_cycle_metrics = self._get_data(cell_name, start_time, end_time)
        # setup dto
        timeseries = self._extract_timeseries(cell_data)
        expansion = self._extract_expansion(cell_data_vdf)
        cycle_metrics = self._extract_cycle_metrics(cell_cycle_metrics, is_ccm=True)
        index_metrics = self._extract_index_metrics(cell_data, cell_data_vdf, cell_cycle_metrics)
        cell_data_dto = CellDataDTO(
            timeseries=timeseries,
            expansion=expansion,
            cycle_metrics=cycle_metrics,
            index_metrics=index_metrics 
        )
        return cell_data_dto
    
    def get_cycle_metrics_AhT(self, cell_name, start_time=None, end_time=None):
        cell_data, cell_data_vdf, cell_cycle_metrics = self._get_data(cell_name, start_time, end_time)
        # setup dto
        timeseries = self._extract_timeseries(cell_data)
        cycle_metrics = self._extract_cycle_metrics(cell_cycle_metrics, is_ccm=True)
        index_metrics = self._extract_index_metrics(cell_data, cell_data_vdf, cell_cycle_metrics, is_ah=True)
        cell_data_dto = CellDataDTO(
            timeseries=timeseries,
            cycle_metrics=cycle_metrics,
            index_metrics=index_metrics 
        )
        return cell_data_dto