#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pika
from pika.exceptions import UnroutableError
from .MQFactory import newConnection

# {
# 'prefetch':1,
# 'exchange':{
	# 'key':'llogs',
	# 'mode':'direct', #fanout direct topics
	# 'durable':True,
# },
# 'queue':{
	# 'key':'hello',
	# 'durable':True,
# },
# 'route':'info'
# }

class MQSubscriber(object):
	data_callback = None;
	def __init__(self, config ,_kwargs):
		# 连接到rabbitmq服务器
		connection = newConnection(_kwargs)
		channel = connection.channel()

		#消费者设置为公平调度
		if 'prefetch' in config:
			channel.basic_qos( prefetch_count = config['prefetch'] )
		
		queue = config['queue']
		#队列
		if 'durable' in queue:
			channel.queue_declare(queue=queue['key'], durable=queue['durable'])
		else:
			channel.queue_declare(queue=queue['key'])
		
		#交换机模式
		if 'exchange' in config:
			exchange = config['exchange']
			if 'durable' in exchange:
				channel.exchange_declare(exchange['key'],exchange['mode'],durable=exchange['durable'])
			else:
				channel.exchange_declare(exchange['key'],exchange['mode'])
			
			#路由
			if 'route' in config:
				channel.queue_bind(queue['key'],exchange['key'],routing_key=config['route'])
			else:
				channel.queue_bind(queue['key'],exchange['key'])
		
		channel.basic_consume(queue['key'],self.callback)
		
		self.channel = channel;
		self.connection = connection;

	def wait(self,done):
		self.data_callback = done
		# 开始接收信息，并进入阻塞状态
		#队列里有信息才会调用callback进行处理
		self.channel.start_consuming()

	def callback(self,ch, method, properties, body):
		# print(" [x] Received %r" % body)
		if self.data_callback != None:
			self.data_callback(body);
		ch.basic_ack(delivery_tag=method.delivery_tag)

	def close(self):
		self.connection.close();
		self.connection = None;
		self.channel = None;

# {
# 'exchange':{
	# 'key':'llogs',
	# 'mode':'direct', #fanout direct topics
	# 'durable':True,
# },
# 'route':'info',
# 'queue':{
	# 'key':'hello',
	# 'durable':True,
# },
# }
class MQPublish(object):
	def __init__(self,config,_kwargs):
		connection = newConnection(_kwargs)

		#创建频道
		channel = connection.channel()

		#交换机模式
		if 'exchange' in config:
			exchange = config['exchange']
			if 'durable' in exchange:
				channel.exchange_declare(exchange['key'],exchange['mode'],durable=exchange['durable'])
			else:
				channel.exchange_declare(exchange['key'],exchange['mode'])
		else:
			queue = config['queue']
			if 'durable' in queue:
				channel.queue_declare(queue=queue['key'], durable=queue['durable'])
			else:
				channel.queue_declare(queue=queue['key'])
		
		channel.confirm_delivery()
		self.config = config;
		self.channel = channel;
		self.connection = connection;

	def publish(self,value):
		print("正在发送...");
		try:
			result = None;
			if 'exchange' in self.config:
				exchange = self.config['exchange']
				route = self.config['route']
				if 'durable' in exchange and exchange['durable']:
					self.channel.basic_publish(exchange=exchange['key'],
									  routing_key=route,
									  body=value,
									  properties=pika.BasicProperties(
										 delivery_mode = 2,
									  ),
									  mandatory=True)
				else:
					self.channel.basic_publish(exchange=exchange['key'],
									  routing_key=route,
									  body=value,
									  mandatory=True)
			else:
				queue = self.config['queue']
				if 'durable' in queue and queue['durable']:
					result = self.channel.basic_publish(exchange='',
										  routing_key=queue['key'],
										  body=value,
										  properties=pika.BasicProperties(
											 delivery_mode = 2,
										  ),
										  mandatory=True)
				else:
					result = self.channel.basic_publish(exchange='',
										  routing_key=queue['key'],
										  body=value,
										  mandatory=True)
		except UnroutableError:
			print('发送失败')
		print("发送完成");

	def close(self):
		self.connection.close();
		self.connection = None;
		self.channel = None;