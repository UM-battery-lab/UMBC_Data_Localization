import os

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_EXPIRATION_TIME = 4*3600 # 4 hours

os.environ['REDIS_HOST'] = REDIS_HOST
os.environ['REDIS_PORT'] = str(REDIS_PORT)