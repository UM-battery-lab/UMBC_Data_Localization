import datetime
from src.config.time_config import TZ_INFO, DATE_FORMAT
from src.utils.SinglentonMeta import SingletonMeta

class DateConverter(metaclass=SingletonMeta):
    def __init__(self):
        self.TZ_INFO = TZ_INFO
        self.DATE_FORMAT = DATE_FORMAT

    def _timestamp_to_datetime(self, t):
        t = t/1000
        return datetime.datetime.fromtimestamp(t, tz=self.TZ_INFO)
    
    def _str_to_timestamp(self, date_str):
        dt = datetime.datetime.strptime(date_str, self.DATE_FORMAT)
        dt = dt.replace(tzinfo=self.TZ_INFO)
        timestamp = dt.timestamp() * 1000
        return timestamp
    
    def _datetime_to_timestamp(self, dt):
        if dt.tzinfo is None:
            dt = dt.astimezone(self.TZ_INFO)  # Convert to the desired timezone
        timestamp = dt.timestamp() * 1000
        return timestamp

    def _str_to_datetime(self, date_str):
        timestamp = self._str_to_timestamp(date_str)
        return self._timestamp_to_datetime(timestamp)
    
    def _format_date_str(self, date_str):
        # Parse the given date string
        date_obj = datetime.datetime.strptime(date_str, '%m/%d/%Y')
        # Convert to the desired format
        return date_obj.strftime(self.DATE_FORMAT)