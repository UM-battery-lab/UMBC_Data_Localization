from matplotlib import pyplot as plt   
from src.utils.Logger import setup_logger
from src.dto.DataTransferObject import CellDataDTO
from src.utils.ObserverPattern import Observer

@Observer
class Viewer():
    """
    The class to view data from the local disk

    Attributes
    ----------
    plt: matplotlib.pyplot object
        The object to plot data
    logger: logger object
        The object to log information
    call_back: function
        The function to call back when the viewer is updated
    
    Methods
    -------
    update(cell_name, measured_data_time, cycle_metrics_time, cycle_metrics_AhT)
        Update the viewer with the data
    plot_process_cell(cell, cell_data: CellDataDTO, downsample = 100)
        Plot the processed data of a cell
    plot_cycle_metrics_time(cell, cell_data: CellDataDTO, downsample = 100)
        Plot the cycle metrics of a cell
    plot_cycle_metrics_AhT(cell, cell_data: CellDataDTO, downsample = 100)
        Plot the cycle metrics of a cell and the AhT
    """
    def __init__(self, call_back = None):
        self.plt = plt 
        self.logger = setup_logger()
        self.call_back = call_back
    
    def update(self, cell_name, measured_data_time, cycle_metrics_time, cycle_metrics_AhT):
        fig_1 = self.plot_process_cell(cell_name, measured_data_time)
        fig_2 = self.plot_cycle_metrics_time(cell_name, cycle_metrics_time)
        fig_3 = self.plot_cycle_metrics_AhT(cell_name, cycle_metrics_AhT)
        if self.call_back:
            self.call_back([fig_1, fig_2, fig_3], cell_name)


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
        
        # plot current 
        ax0 = axes.flat[0]
        ax0.plot_date(t[0::downsample],I[0::downsample],'-')
        # ax0.plot_date(t[0::downsample],step_idx[0::downsample],'-')
        ax0.plot_date(t[cycle_idx], I[cycle_idx], "x")
        ax0.plot_date(t[capacity_check_idx], I[capacity_check_idx], "*", c = "r")
        ax0.set_ylabel("Current [A]")
        ax0.grid()

        # plot voltage 
        ax1 = axes.flat[1]
        ax1.plot_date(t[0::downsample],V[0::downsample],'-')
        ax1.plot_date(t[cycle_idx], V[cycle_idx], "x")
        ax1.plot_date(t[charge_idx], V[charge_idx], "o")
        ax1.plot_date(t[capacity_check_idx], V[capacity_check_idx], "*", c = "r")
        ax1.set_ylabel("Voltage [V]")
        ax1.grid()

        # plot temperature
        ax2 = axes.flat[2]
        ax2.plot_date(t[0::downsample],T[0::downsample],'-')
        # ax2.plot_date(t_vdf[0::downsample],T_vdf[0::downsample],'-', c='g')
        ax2.plot_date(t_vdf[0::downsample],T_vdf[0::downsample],'--', c='grey')
        ax2.plot_date(t[capacity_check_idx], T[capacity_check_idx], "*", c = "r")
        ax2.plot_date(t[cycle_idx], T[cycle_idx], "x")
        ax2.set_ylabel("Temperature \n [degC]")
        ax2.grid()

        # set up expansion plot with points aligning with the end of the cycle
        ax3 = axes.flat[3]
        if not all(t_vdf.isnull()):
            ax3.plot_date(t_vdf[0::100],exp_vdf[0::100],'-')
            ax3.plot_date(t_vdf[cycle_idx_vdf], exp_vdf[cycle_idx_vdf], "x")
        ax3.set_ylabel("Expansion [um]")
        ax3.grid()

        # plot AhT 
        ax4 = axes.flat[4]
        ax4.plot_date(t[0::downsample],AhT[0::downsample],'-')
        ax4.plot_date(t[cycle_idx], AhT[cycle_idx], "x")
        ax4.set_ylabel("AhT [A.h]")
        ax4.grid()
        
        # plot capacity 
        ax5 = axes.flat[5]
        ax5.plot_date(t_cycle,Q_c)
        ax5.plot_date(t_cycle,Q_d)
        ax5.plot_date(t[capacity_check_idx],Q_c[capacity_check_in_cycle_idx], "*", c = "r")
        ax5.plot_date(t[capacity_check_idx],Q_d[capacity_check_in_cycle_idx], "*", c = "r")
        ax5.set_ylabel("Apparent \n capacity [A.h]")
        ax5.grid()

        fig.autofmt_xdate()
        fig.suptitle("Cell: "+cell)
        fig.tight_layout()
        self.plt.ion()
        self.plt.show()
        return fig
    
    def plot_cycle_metrics_time(self, cell, cell_data: CellDataDTO, downsample = 100):
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
        V_min = cycle_metrics.V_min
        V_max = cycle_metrics.V_max
        T_min = cycle_metrics.T_min
        T_max = cycle_metrics.T_max
        exp_min = cycle_metrics.exp_min
        exp_max = cycle_metrics.exp_max
        exp_rev = cycle_metrics.exp_rev

        cycle_idx = index_metrics.cycle_idx
        capacity_check_idx = index_metrics.capacity_check_idx
        cycle_idx_vdf = index_metrics.cycle_idx_vdf
        capacity_check_in_cycle_idx = index_metrics.capacity_check_in_cycle_idx
        charge_idx = index_metrics.charge_idx

        self.logger.info("Plotting cycle metrics: " + cell)
        # setup plot 
        fig, axes = self.plt.subplots(3,4,figsize=(18,6), sharex=True)

        # plot current 
        ax0 = axes.flat[0]
        ax0.plot_date(t[0::downsample],I[0::downsample],'-')
        ax0.plot_date(t[cycle_idx], I[cycle_idx], "x")
        ax0.plot_date(t[charge_idx], I[charge_idx], "o")
        ax0.plot_date(t[capacity_check_idx], I[capacity_check_idx], "*", c = "r")
        ax0.set_ylabel("Current[A]")
        ax0.grid()

        # plot voltage 
        ax1 = axes.flat[1]
        ax1.plot_date(t[0::downsample],V[0::downsample],'-')
        ax1.plot_date(t[cycle_idx], V[cycle_idx], "x")
        ax1.plot_date(t[charge_idx], V[charge_idx], "o")
        ax1.plot_date(t[capacity_check_idx], V[capacity_check_idx], "*", c = "r")
        ax1.set_ylabel("Voltage [V]")
        ax1.grid()

        # plot temperature
        ax2 = axes.flat[2]
        ax2.plot_date(t[0::downsample],T[0::downsample],'-')
        # ax2.plot_date(t_vdf[0::downsample],T_vdf[0::downsample],'-',c='g')
        ax2.plot_date(t_vdf[0::downsample],T_vdf[0::downsample],'--', c='grey')
        ax2.plot_date(t[cycle_idx], T[cycle_idx], "x")
        ax2.plot_date(t[capacity_check_idx], T[capacity_check_idx], "*", c = "r")
        ax2.set_ylabel("Temp [degC]")
        ax2.grid()

        # plot exponsion
        ax3 = axes.flat[3]
        if not all(t_vdf.isnull()):
            ax3.plot_date(t_vdf[0::downsample],exp_vdf[0::downsample],'-') 
            # ax3.plot_date(t_vdf[0::downsample],exp_vdf_ref[0::downsample],'--', c='grey') 
            ax3.plot_date(t_vdf[cycle_idx_vdf], exp_vdf[cycle_idx_vdf], "x")
            # ax3.plot_date(t_vdf[cycle_idx_vdf], exp_vdf[cycle_idx_vdf], "x")
        ax3.set_ylabel("Expansion [um]")
        ax3.grid()

        # plot AhT 
        ax4 = axes.flat[4]
        ax4.plot_date(t[0::downsample],AhT[0::downsample],'-')
        ax4.plot_date(t[cycle_idx], AhT[cycle_idx], "x")
        ax4.plot_date(t[capacity_check_idx], AhT[capacity_check_idx], "*", c = "r")
        ax4.set_ylabel("Ah Throughput")
        ax4.grid()

        # plot capacity 
        ax8 = axes.flat[8]
        ax8.plot_date(t_cycle,Q_c)
        ax8.plot_date(t_cycle,Q_d)
        ax8.plot_date(t[capacity_check_idx],Q_c[capacity_check_in_cycle_idx], "*", c = "r")
        ax8.plot_date(t[capacity_check_idx],Q_d[capacity_check_in_cycle_idx], "*", c = "r")
        ax8.set_ylabel("Apparent \n capacity [A.h]")
        ax8.grid()

        # plot min/max voltage 
        ax5 = axes.flat[5]
        ax5.plot_date(t_cycle,V_max)
        ax5.plot_date(t[capacity_check_idx],V_max[capacity_check_in_cycle_idx], "*", c = "r")
        ax5.set_ylabel("Max Voltage [V]")
        ax5.grid()

        ax9 = axes.flat[9]
        ax9.plot_date(t_cycle,V_min)
        ax9.plot_date(t[capacity_check_idx],V_min[capacity_check_in_cycle_idx], "*", c = "r")
        ax9.set_ylabel("Min Voltage [V]")
        ax9.grid()

        # plot min/max temperature
        ax6 = axes.flat[6]
        ax6.plot_date(t_cycle,T_max)
        ax6.plot_date(t[capacity_check_idx],T_max[capacity_check_in_cycle_idx], "*", c = "r")
        ax6.set_ylabel("Max Temp [degC]")
        ax6.grid()

        ax10 = axes.flat[10]
        ax10.plot_date(t_cycle,T_min)
        ax10.plot_date(t[capacity_check_idx],T_min[capacity_check_in_cycle_idx], "*", c = "r")
        ax10.set_ylabel("Min Temp [degC]")
        ax10.grid()

        # plot min/max/rev expansion  
        ax7 = axes.flat[7]
        ax7.plot_date(t_cycle, exp_max) 
        ax7.plot_date(t_cycle, exp_min)
        ax7.plot_date(t_cycle[capacity_check_in_cycle_idx], exp_max[capacity_check_in_cycle_idx], "*", c = "r") 
        ax7.plot_date(t_cycle[capacity_check_in_cycle_idx], exp_min[capacity_check_in_cycle_idx], "*", c = "r")
        ax7.set_ylabel("Min/Max Exp [um]")
        ax7.grid()

        ax15 = axes.flat[11]
        ax15.plot_date(t_cycle, exp_rev)
        ax15.plot_date(t_cycle[capacity_check_in_cycle_idx], exp_rev[capacity_check_in_cycle_idx], "*", c = "r")
        ax15.set_ylabel("Rev expansion [um]")
        ax15.grid()

        fig.autofmt_xdate()
        fig.suptitle("Cell: "+cell)
        fig.tight_layout()
        self.plt.ion()
        self.plt.show()
        return fig

    def plot_cycle_metrics_AhT(self, cell, cell_data: CellDataDTO, downsample = 100):
        timeseries = cell_data.timeseries
        cycle_metrics = cell_data.cycle_metrics
        index_metrics = cell_data.index_metrics

        I = timeseries.I
        V = timeseries.V
        T = timeseries.T
        AhT = timeseries.AhT

        Q_c = cycle_metrics.Q_c
        Q_d = cycle_metrics.Q_d
        AhT_cycle = cycle_metrics.AhT_cycle
        V_min = cycle_metrics.V_min
        V_max = cycle_metrics.V_max
        T_min = cycle_metrics.T_min
        T_max = cycle_metrics.T_max
        exp_min = cycle_metrics.exp_min
        exp_max = cycle_metrics.exp_max
        exp_rev = cycle_metrics.exp_rev

        cycle_idx = index_metrics.cycle_idx
        capacity_check_idx = index_metrics.capacity_check_idx
        capacity_check_in_cycle_idx = index_metrics.capacity_check_in_cycle_idx

        self.logger.info("Plotting cycle metrics AhT: " + cell)

        # setup plot 
        fig, axes = self.plt.subplots(3,4,figsize=(12,6), sharex=True)

        # plot current 
        ax0 = axes.flat[0]
        ax0.plot(AhT[0::downsample],I[0::downsample], zorder=1)
        ax0.scatter(AhT[cycle_idx], I[cycle_idx], marker = "x", c = "m",zorder=2)
        ax0.scatter(AhT[capacity_check_idx], I[capacity_check_idx], marker = "*", c = "r",zorder=3)
        ax0.set_ylabel("Current[A]")
        ax0.grid()

        # plot voltage 
        ax1 = axes.flat[1]
        ax1.plot(AhT[0::downsample],V[0::downsample],zorder=1)
        ax1.scatter(AhT[cycle_idx], V[cycle_idx], marker = "x", c = "m", zorder=2)
        ax1.scatter(AhT[capacity_check_idx], V[capacity_check_idx], marker = "*", c = "r",zorder=3)
        ax1.set_ylabel("Voltage [V]")
        ax1.grid()

        # plot temperature
        ax2 = axes.flat[2]
        ax2.plot(AhT[0::downsample],T[0::downsample], zorder=1)
        ax2.scatter(AhT[capacity_check_idx], T[capacity_check_idx], marker = "*", c = "r",zorder=3)
        ax2.set_ylabel("Temp [degC]")
        # ax2.xaxis.set_major_locator(x_label_spacing)
        ax2.grid()

        # plot capacity 
        ax8 = axes.flat[8]
        ax8.scatter(AhT_cycle,Q_c, marker = "x", c = "g")
        ax8.scatter(AhT_cycle,Q_d, facecolors='none', edgecolors='c')
        ax8.scatter(AhT[capacity_check_idx], Q_c[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=3)
        ax8.scatter(AhT[capacity_check_idx], Q_d[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=4)
        ax8.set_ylabel("Apparent \n capacity [A.h]")
        ax8.grid()

        # plot min/max voltage 
        ax5 = axes.flat[5]
        # x,y = reject_outliers(AhT_cycle,V_max, m = reject_outliers_std)
        ax5.scatter(AhT_cycle,V_max)
        ax5.scatter(AhT[capacity_check_idx], V_max[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=3)
        ax5.set_ylabel("Max Voltage [V]")
        # ax5.xaxis.set_major_locator(x_label_spacing)
        ax5.grid()

        ax9 = axes.flat[9]
        ax9.scatter(AhT_cycle,V_min)
        ax9.scatter(AhT[capacity_check_idx], V_min[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=3)
        ax9.set_ylabel("Min Voltage [V]")
        ax9.grid()
        ax9.set_xlabel("Ah Throughput [A.h]")

        # plot min/max temperature
        ax6 = axes.flat[6]
        ax6.scatter(AhT_cycle, T_max)   
        ax6.scatter(AhT[capacity_check_idx], T_max[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=3)
        ax6.set_ylabel("Max Temp [degC]")
        ax6.grid()

        ax10 = axes.flat[10]
        ax10.scatter(AhT_cycle, T_min) 
        ax10.scatter(AhT[capacity_check_idx], T_min[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=3)
        ax10.set_ylabel("Min Temp [degC]")
        ax10.grid()
        ax10.set_xlabel("Ah Throughput [A.h]")

        # plot min/max/rev expansion 
        ax7 = axes.flat[7]
        ax7.scatter(AhT_cycle, exp_max)
        ax7.scatter(AhT_cycle, exp_min)
        ax7.scatter(AhT[capacity_check_idx], exp_max[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=3)
        ax7.scatter(AhT[capacity_check_idx], exp_min[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=3)
        ax7.set_ylabel("Max/ Min Exp [-]")
        ax7.grid()
            
        ax11 = axes.flat[11]
        ax11.scatter(AhT_cycle, exp_rev)
        ax11.scatter(AhT[capacity_check_idx], exp_rev[capacity_check_in_cycle_idx], marker = "*", c = "r",zorder=3)
        ax11.set_ylabel("Rev Exp [um]")
        ax11.set_xlabel('Ah Throughput')
        ax11.grid()

        fig.autofmt_xdate()
        fig.suptitle("Cell: "+cell)
        fig.tight_layout()
        self.plt.ion()
        self.plt.show()
        return fig