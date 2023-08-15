from typing import NamedTuple, Optional
import pandas as pd

class TimeSeriesDTO(NamedTuple):
    t: pd.Series
    I: pd.Series
    V: pd.Series
    T: pd.Series
    AhT: pd.Series

class ExpansionDTO(NamedTuple):
    t_vdf: pd.Series
    exp_vdf: pd.Series
    T_vdf: pd.Series

class CycleMetricsDTO(NamedTuple):
    t_cycle: pd.Series
    Q_c: pd.Series
    Q_d: pd.Series
    AhT_cycle: Optional[pd.Series]=None
    V_min: Optional[pd.Series]=None
    V_max: Optional[pd.Series]=None 
    T_min: Optional[pd.Series]=None
    T_max: Optional[pd.Series]=None
    exp_min: Optional[pd.Series]=None 
    exp_max: Optional[pd.Series]=None 
    exp_rev: Optional[pd.Series]=None

class IndexMetricsDTO(NamedTuple):
    cycle_idx: pd.Index
    capacity_check_idx: pd.Index
    capacity_check_in_cycle_idx: pd.Index
    cycle_idx_vdf: Optional[pd.Index]=None
    charge_idx: Optional[pd.Index]=None


class CellDataDTO(NamedTuple):
    timeseries: TimeSeriesDTO
    expansion: ExpansionDTO
    cycle_metrics: CycleMetricsDTO
    index_metrics: IndexMetricsDTO

