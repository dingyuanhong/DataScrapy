# -*- coding: utf-8 -*-

import redis

class RedisCache:
	pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
	cache = redis.Redis(connection_pool=pool)

	def get(self,name,default):
		value = self.cache.get(name);
		if date == None:
			return default;
		return str(date.decode('utf-8'));

	def set(self,name,value):
		return self.cache.set(name,value);


Cache = RedisCache();