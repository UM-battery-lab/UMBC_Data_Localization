from src.dto.DataTransferObject import TimeSeriesDTO, ExpansionDTO, CycleMetricsDTO, IndexMetricsDTO, CellDataDTO
from src.utils.Logger import setup_logger
from src.utils.DateConverter import DateConverter
from src.utils.ObserverPattern import Observer, Subject

class Presenter(Observer, Subject):
    """
    Presenter class to present data to the frontend

    Attributes
    ----------
    dateConverter: DateConverter object
        The object to convert date
    logger: logger object
        The object to log information

    Methods
    -------
    attach(observer: Observer)
        Attach an observer to the Presenter.
    detach(observer: Observer)
        Detach an observer from the Presenter.
    notify(*args, **kwargs)
        Notify all observers about an event.
    update(cell_name, cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt)
        Get the data from the data manager and notify the viewer
    get_measured_data_time(cell_cycle_metrics, cell_data, cell_data_vdf, plot_cycles = True)
        Get measured data for a cell
    get_cycle_metrics_time(cell_cycle_metrics, cell_data, cell_data_vdf)
        Get cycle metrics for a cell
    get_cycle_metrics_AhT(cell_cycle_metrics, cell_data, cell_data_vdf)
        Get cycle metrics for a cell
    """
    def __init__(self):
        self.dateConverter = DateConverter()
        self.logger = setup_logger()
        self._observers = []

    def attach(self, observer: Observer):
        self._observers.append(observer)

    def detach(self, observer: Observer):
        self._observers.remove(observer)

    def notify(self, *args, **kwargs):
        for observer in self._observers:
            observer.update(*args, **kwargs)

    def update(self, cell_name, cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt):
        #TODO: Get better way to process data to avoid excuting 'Time [s]'.apply(self.dateConverter._timestamp_to_datetime) multiple times
        measured_data_time = self.get_measured_data_time(cell_cycle_metrics, cell_data, cell_data_vdf)
        cycle_metrics_time = self.get_cycle_metrics_time(cell_cycle_metrics, cell_data, cell_data_vdf)
        cycle_metrics_AhT = self.get_cycle_metrics_AhT(cell_cycle_metrics, cell_data, cell_data_vdf)
        self.notify(cell_name, measured_data_time, cycle_metrics_time, cycle_metrics_AhT)
    
    
    def _extract_timeseries(self, data) -> TimeSeriesDTO:
        return TimeSeriesDTO(
            t=data['Time [s]'].apply(self.dateConverter._timestamp_to_datetime),
            I=data['Current [A]'],
            V=data['Voltage [V]'],
            T=data['Temperature [degC]'],
            AhT=data['Ah throughput [A.h]']
        )
    
    def _extract_expansion(self, data) -> ExpansionDTO:
        return ExpansionDTO(
            t_vdf=data['Time [s]'].apply(self.dateConverter._timestamp_to_datetime),
            exp_vdf=data['Expansion [-]'],
            T_vdf=data['Temperature [degC]']
        )
    
    def _extract_cycle_metrics(self, data, is_ccm=False) -> CycleMetricsDTO:
        if is_ccm:
            return CycleMetricsDTO(
                t_cycle=data['Time [s]'].apply(self.dateConverter._timestamp_to_datetime),
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
            t_cycle=data['Time [s]'].apply(self.dateConverter._timestamp_to_datetime),
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

    def get_measured_data_time(self, cell_cycle_metrics, cell_data, cell_data_vdf, plot_cycles = True): 
        """
        Get measured data for a cell

        Parameters
        ----------
        cell_cycle_metrics: pandas Dataframe
            The dataframe of cell cycle metrics
        cell_data: pandas Dataframe
            The dataframe of cell data
        cell_data_vdf: pandas Dataframe
            The dataframe of cell data vdf
        plot_cycles: bool, optional
            Whether to plot the cycles
        
        Returns
        -------
        CellDataDTO
            The data transfer object of the cell data
        """
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

    def get_cycle_metrics_time(self, cell_cycle_metrics, cell_data, cell_data_vdf):
        """
        Get cycle metrics for a cell

        Parameters
        ----------
        cell_cycle_metrics: pandas Dataframe
            The dataframe of cell cycle metrics
        cell_data: pandas Dataframe
            The dataframe of cell data
        cell_data_vdf: pandas Dataframe
            The dataframe of cell data vdf
        
        Returns
        -------
        CellDataDTO
            The data transfer object of the cell data
        """
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
    
    def get_cycle_metrics_AhT(self, cell_cycle_metrics, cell_data, cell_data_vdf):
        """
        Get cycle metrics for a cell

        Parameters
        ----------
        cell_cycle_metrics: pandas Dataframe
            The dataframe of cell cycle metrics
        cell_data: pandas Dataframe
            The dataframe of cell data
        cell_data_vdf: pandas Dataframe
            The dataframe of cell data vdf
        
        Returns
        -------
        CellDataDTO
            The data transfer object of the cell data  
        """
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