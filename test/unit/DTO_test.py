import unittest
import pandas as pd
from src.dto.DataTransferObject import TimeSeriesDTO, ExpansionDTO, CycleMetricsDTO, IndexMetricsDTO, CellDataDTO

class TestDTOs(unittest.TestCase):

    def create_sample_series(self):
        return pd.Series([1, 2, 3, 4, 5])

    def create_sample_index(self):
        return pd.Index([1, 2, 3])

    def test_time_series_dto(self):
        series = self.create_sample_series()
        ts_dto = TimeSeriesDTO(t=series, I=series, V=series, T=series, AhT=series)
        
        self.assertIsInstance(ts_dto.t, pd.Series)
        self.assertIsInstance(ts_dto.I, pd.Series)
        self.assertIsInstance(ts_dto.V, pd.Series)
        self.assertIsInstance(ts_dto.T, pd.Series)
        self.assertIsInstance(ts_dto.AhT, pd.Series)

    def test_expansion_dto(self):
        series = self.create_sample_series()
        exp_dto = ExpansionDTO(t_vdf=series, exp_vdf=series, T_vdf=series)
        
        self.assertIsInstance(exp_dto.t_vdf, pd.Series)
        self.assertIsInstance(exp_dto.exp_vdf, pd.Series)
        self.assertIsInstance(exp_dto.T_vdf, pd.Series)

    def test_cycle_metrics_dto(self):
        series = self.create_sample_series()
        cm_dto = CycleMetricsDTO(t_cycle=series, Q_c=series, Q_d=series)
        
        self.assertIsInstance(cm_dto.t_cycle, pd.Series)
        self.assertIsInstance(cm_dto.Q_c, pd.Series)
        self.assertIsInstance(cm_dto.Q_d, pd.Series)

    def test_index_metrics_dto(self):
        index = self.create_sample_index()
        im_dto = IndexMetricsDTO(cycle_idx=index, capacity_check_idx=index, cycle_idx_vdf=index, capacity_check_in_cycle_idx=index, charge_idx=index)

        self.assertIsInstance(im_dto.cycle_idx, pd.Index)
        self.assertIsInstance(im_dto.capacity_check_idx, pd.Index)
        self.assertIsInstance(im_dto.cycle_idx_vdf, pd.Index)
        self.assertIsInstance(im_dto.capacity_check_in_cycle_idx, pd.Index)
        self.assertIsInstance(im_dto.charge_idx, pd.Index)

    def test_cell_data_dto(self):
        series = self.create_sample_series()
        index = self.create_sample_index()

        ts_dto = TimeSeriesDTO(t=series, I=series, V=series, T=series, AhT=series)
        exp_dto = ExpansionDTO(t_vdf=series, exp_vdf=series, T_vdf=series)
        cm_dto = CycleMetricsDTO(t_cycle=series, Q_c=series, Q_d=series)
        im_dto = IndexMetricsDTO(cycle_idx=index, capacity_check_idx=index, cycle_idx_vdf=index, capacity_check_in_cycle_idx=index, charge_idx=index)

        cell_dto = CellDataDTO(timeseries=ts_dto, expansion=exp_dto, cycle_metrics=cm_dto, index_metrics=im_dto)

        self.assertIsInstance(cell_dto.timeseries, TimeSeriesDTO)
        self.assertIsInstance(cell_dto.expansion, ExpansionDTO)
        self.assertIsInstance(cell_dto.cycle_metrics, CycleMetricsDTO)
        self.assertIsInstance(cell_dto.index_metrics, IndexMetricsDTO)

if __name__ == "__main__":
    unittest.main()
