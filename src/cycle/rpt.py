import pandas as pd


def summarize_rpt_data(cell_data, cell_data_vdf, cell_cycle_metrics):
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
    cell_rpt_data = cell_rpt_data[[cols[len(cols)-1]] + cols[0:-1]] 

    return cell_rpt_data