import os
from datetime import timedelta, timezone

DATE_FORMAT = '%Y-%m-%d_%H-%M-%S'
ROOT_PATH = os.path.join(os.path.dirname(os.getcwd()), 'voltaiq_data')
JSON_FILE_PATH = os.path.join(ROOT_PATH, 'directory_structure.json')
TZ_INFO = timezone(timedelta(days=-1, seconds=72000))
TIME_TOLERANCE = timedelta(hours=2)
CYCLE_ID_LIMS= {
    'RPT': {'V_max_cycle':4.1, 'V_min_cycle':3.8, 'dt_min': 600, 'dAh_min':0.1},
    'CYC': {'V_max_cycle':3.8, 'V_min_cycle':3.8, 'dt_min': 600, 'dAh_min':0.1},
    'Test11': {'V_max_cycle':3.6, 'V_min_cycle':3.6, 'dt_min': 600, 'dAh_min':0.1},
    'EIS': {'V_max_cycle':4.1, 'V_min_cycle':3.8, 'dt_min': 600, 'dAh_min':0.5}, # same as RPT, but says EIS in the filenames for some GM cells
    'CAL': {'V_max_cycle':3.8, 'V_min_cycle':3.8, 'dt_min': 600, 'dAh_min':0.5},
    '_F': {'V_max_cycle':3.8, 'V_min_cycle':3.8, 'dt_min': 3600, 'dAh_min':0.5} # Formation files handled via peak finding
}
DEFAULT_TRACE_KEYS = ['h_datapoint_time', 'h_test_time', 'h_current', 'h_potential', 'c_cumulative_capacity', 
                    'aux_neware_xls_t1_none_0', 'h_step_index']
DEFAULT_DF_LABELS = ['Time [s]', 'Test Time [s]', 'Current [A]', 'Voltage [V]', 'Ah throughput [A.h]', 
                    'Temperature [degC]', 'Step index']
TIME_COLUMNS = ['h_datapoint_time', 'aux_vdf_timestamp_datetime_0', 'aux_vdf_timestamp_epoch_0']