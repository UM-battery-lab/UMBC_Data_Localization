import redis
import pickle
import subprocess
from src.config import redis_config
from src.utils.SinglentonMeta import SingletonMeta
from src.utils.Logger import setup_logger

class RedisClient(metaclass=SingletonMeta):
    def __init__(self):
        self.logger = setup_logger()
        self._ensure_redis_running()
        self.client = redis.Redis(host=redis_config.REDIS_HOST, 
                                  port=redis_config.REDIS_PORT, 
                                  db=redis_config.REDIS_DB)
    # TODO: This is a temporary solution. Need to put it into docker-compose in the future
    def _ensure_redis_running(self):
        try:
            test_client = redis.Redis(host=redis_config.REDIS_HOST, 
                                      port=redis_config.REDIS_PORT, 
                                      db=redis_config.REDIS_DB)
            test_client.ping() # This will raise an exception if the server is not running
        except redis.ConnectionError:
            self.logger.warning("Redis server not running. Attempting to start...")
            try:
                subprocess.Popen(["redis-server"])
                self.logger.info("Redis server started.")
            except Exception as e:
                self.logger.error(f"Error starting Redis server: {e}")
                raise

    def set_pickle(self, key, value):
        try:
            serialized_data = pickle.dumps(value)
            self.client.set(key, serialized_data)
            self.client.expire(key, redis_config.REDIS_EXPIRATION_TIME)
        except Exception as e:
            self.logger.error(f'Error while setting pickle to redis: {e}')

    def get_pickle(self, key):
        try:
            serialized_data = self.client.get(key)
            if serialized_data:
                return pickle.loads(serialized_data)
        except Exception as e:
            self.logger.error(f'Error while getting pickle from redis: {e}')
        return None

    def delete(self, key):
        try:
            self.client.delete(key)
        except Exception as e:
            self.logger.error(f'Error while deleting key from redis: {e}')