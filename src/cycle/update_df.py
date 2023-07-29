import pandas as pd

def update_dataframe(df, df_new, file_start_time, file_end_time, update_AhT = True): 
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
