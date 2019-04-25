# -*- coding: utf-8 -*-

import redis
from scrapy.conf import settings
import logging

class RedisCache:
	HOST = settings.get('REDIS_HOST')
	PORT = settings.get('REDIS_PORT')
	pool = redis.ConnectionPool(host=HOST, port=PORT)
	cache = redis.Redis(connection_pool=pool)

	def get(self,name,default):
		value = self.cache.get(name);
		if value == None:
			return default;
		return str(value.decode('utf-8'));

	def set(self,name,value):
		return self.cache.set(name,value);


Cache = RedisCache();