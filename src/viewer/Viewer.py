from matplotlib import pyplot as plt   
from src.logger_config import setup_logger

class Viewer:
    def __init__(self):
        self.plt = plt 
        self.logger = setup_logger()

    def plot_measured_data_time(self, cell, data_dict, downsample = 100):
        # setup timeseries data
        t = data_dict['t']
        I = data_dict['I']
        V = data_dict['V']
        T = data_dict['T']
        AhT = data_dict['AhT']
        t_vdf = data_dict['t_vdf']
        exp_vdf = data_dict['exp_vdf']
        T_vdf = data_dict['T_vdf']
        # T_amb = cell_data_vdf['Amb Temp [degC]']
        # setup cycle metrics
        t_cycle = data_dict['t_cycle']
        Q_c = data_dict['Q_c']
        Q_d = data_dict['Q_d']

        cycle_idx = data_dict['cycle_idx']
        capacity_check_idx = data_dict['capacity_check_idx']
        cycle_idx_vdf = data_dict['cycle_idx_vdf']
        capacity_check_in_cycle_idx = data_dict['capacity_check_in_cycle_idx']
        charge_idx = data_dict['charge_idx']
        
        # setup plot 
        fig, axes = self.plt.subplots(6,1,figsize=(6,6), sharex=True)

        self.__plot_with_axis(axes.flat[0], t, I, "Current [A]", cycle_idx, capacity_check_idx)
        self.__plot_with_axis(axes.flat[1], t, V, "Voltage [V]", cycle_idx, capacity_check_idx, special_t=t, special_data=V[charge_idx], marker='o')
        self.__plot_with_axis(axes.flat[2], t, T, "Temperature [degC]", cycle_idx, capacity_check_idx, special_t2=t_vdf, special_data2=T_vdf, linestyle='--')
        if not all(t_vdf.isnull()):
            self.__plot_with_axis(axes.flat[3], t_vdf, exp_vdf, "Expansion [-]", cycle_idx_vdf)
        self.__plot_with_axis(axes.flat[4], t, AhT, "AhT [A.h]", cycle_idx)
        self.__plot_with_axis(axes.flat[5], t_cycle, Q_c, "Apparent \n capacity [A.h]", capacity_check_idx=capacity_check_in_cycle_idx)
        self.__plot_with_axis(axes.flat[5], t_cycle, Q_d, "Apparent \n capacity [A.h]", capacity_check_idx=capacity_check_in_cycle_idx)

        fig.autofmt_xdate()
        fig.suptitle("Cell: " + cell)
        fig.tight_layout()
        plt.show()
        

        
    def __plot_with_axis(self, ax, t, data, ylabel, cycle_idx=None, capacity_check_idx=None, special_t=None, special_data=None, marker=None, special_t2=None, special_data2=None, linestyle=None, downsample=100):
        ax.plot_date(t[0::downsample], data[0::downsample], '-')
        if cycle_idx is not None:
            ax.plot_date(t[cycle_idx], data[cycle_idx], 'x')
        if capacity_check_idx is not None:
            ax.plot_date(t[capacity_check_idx], data[capacity_check_idx], '*', c='r')
        if special_t is not None and special_data is not None:
            ax.plot_date(special_t, special_data, marker)
        if special_t2 is not None and special_data2 is not None:
            ax.plot_date(special_t2[0::downsample], special_data2[0::downsample], linestyle, c='grey')
        ax.set_ylabel(ylabel)
        ax.grid()

