from datetime import timedelta, timezone

DATE_FORMAT = '%Y-%m-%d_%H-%M-%S'
TZ_INFO = timezone(timedelta(days=-1, seconds=72000))
TIME_TOLERANCE = timedelta(hours=2)