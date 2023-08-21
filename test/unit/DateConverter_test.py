import unittest
import datetime
from src.utils.DateConverter import DateConverter

class TestDateConverter(unittest.TestCase):

    def setUp(self):
        self.converter = DateConverter()
        self.sample_datetime = datetime.datetime(2023, 7, 25, 12, 4, 16, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=72000)))
        self.sample_string = "2023-7-25_12-4-16"
        self.sample_timestamp = self.converter._str_to_timestamp(self.sample_string)

    def test_str_to_timestamp(self):
        timestamp = self.converter._str_to_timestamp(self.sample_string)
        self.assertEqual(timestamp, self.sample_timestamp)

    def test_timestamp_to_datetime(self):
        dt = self.converter._timestamp_to_datetime(self.sample_timestamp)
        self.assertEqual(dt, self.sample_datetime)

    def test_datetime_to_timestamp(self):
        timestamp = self.converter._datetime_to_timestamp(self.sample_datetime)
        self.assertEqual(timestamp, self.sample_timestamp)


if __name__ == '__main__':
    unittest.main()