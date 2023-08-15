from matplotlib import pyplot as plt   
from src.logger_config import setup_logger
from src.dto.DataTransferObject import TimeSeriesDTO, ExpansionDTO, CycleMetricsDTO, IndexMetricsDTO, CellDataDTO

#TODO: clean the structure of the code
class Viewer:
    def __init__(self):
        self.plt = plt 
        self.logger = setup_logger()

    def plot_process_cell(self, cell, cell_data: CellDataDTO, downsample = 100):
        timeseries = cell_data.timeseries
        expansion = cell_data.expansion
        cycle_metrics = cell_data.cycle_metrics
        index_metrics = cell_data.index_metrics

        t = timeseries.t
        I = timeseries.I
        V = timeseries.V
        T = timeseries.T
        AhT = timeseries.AhT

        t_vdf = expansion.t_vdf
        exp_vdf = expansion.exp_vdf
        T_vdf = expansion.T_vdf

        t_cycle = cycle_metrics.t_cycle
        Q_c = cycle_metrics.Q_c
        Q_d = cycle_metrics.Q_d

        cycle_idx = index_metrics.cycle_idx
        capacity_check_idx = index_metrics.capacity_check_idx
        cycle_idx_vdf = index_metrics.cycle_idx_vdf
        capacity_check_in_cycle_idx = index_metrics.capacity_check_in_cycle_idx
        charge_idx = index_metrics.charge_idx
        
        self.logger.info("Plotting cell: " + cell)
        # setup plot 
        fig, axes = self.plt.subplots(6,1,figsize=(6,6), sharex=True)

        self.__plot_with_axis(axes.flat[0], t, I, "Current [A]", cycle_idx, charge_idx, capacity_check_idx, downsample=downsample)
        self.__plot_with_axis(axes.flat[1], t, V, "Voltage [V]", cycle_idx, charge_idx, capacity_check_idx, downsample=downsample)
        self.__plot_with_axis(axes.flat[2], t, T, "Temperature [degC]", cycle_idx, capacity_check_idx, special_t2=t_vdf, special_data2=T_vdf, linestyle='--', downsample=downsample)
        if not all (t_vdf.isnull()):
            self.__plot_with_axis(axes.flat[3], t_vdf, exp_vdf, "Expansion [-]", cycle_idx_vdf, downsample=downsample)
        self.__plot_with_axis(axes.flat[4], t, AhT, "AhT [A.h]", cycle_idx)
        self.__plot_with_axis(axes.flat[5], t_cycle, Q_c, "Apparent \n capacity [A.h]", capacity_check_idx=capacity_check_in_cycle_idx, downsample=downsample)
        self.__plot_with_axis(axes.flat[5], t_cycle, Q_d, "Apparent \n capacity [A.h]", capacity_check_idx=capacity_check_in_cycle_idx, downsample=downsample)

        # fig.autofmt_xdate()
        fig.suptitle("Cell: " + cell)
        fig.tight_layout()
        plt.show()
        

    def __plot_with_axis(self, ax, t, data, ylabel, cycle_idx=None, charge_idx=None, capacity_check_idx=None, special_t=None, special_data=None, marker=None, special_t2=None, special_data2=None, linestyle=None, downsample=100):
        ax.plot_date(t[0::downsample], data[0::downsample], '-')
        
        def safe_plot_date(indices, *args, **kwargs):
            valid_indices = [idx for idx in indices if idx in t.index]
            if valid_indices:
                ax.plot_date(t[valid_indices], data[valid_indices], *args, **kwargs)
            else:
                self.logger.warning(f'No valid indices for {args[0]}')
        
        if cycle_idx is not None:
            safe_plot_date(cycle_idx, 'x')
        if charge_idx is not None:
            safe_plot_date(charge_idx, 'o')
        if capacity_check_idx is not None:
            safe_plot_date(capacity_check_idx, '*', c='r')
        if special_t is not None and special_data is not None:
            ax.plot_date(special_t, special_data, marker)
        if special_t2 is not None and special_data2 is not None:
            ax.plot_date(special_t2[0::downsample], special_data2[0::downsample], linestyle, c='grey')
        
        ax.set_ylabel(ylabel)
        ax.grid()
