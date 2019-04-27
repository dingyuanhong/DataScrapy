#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pika

#https://www.cnblogs.com/pangguoping/p/5720134.html

def newConnection(ARGS):
	# ######################### 生产者 #########################
	credentials = pika.PlainCredentials(ARGS['credentials']['name'], ARGS['credentials']['passwd'])
	#链接rabbit服务器（localhost是本机，如果是其他服务器请修改为ip地址）
	connection = pika.BlockingConnection(pika.ConnectionParameters(ARGS['HOST'],ARGS['PORT'],ARGS['VHOST'],credentials))
	return connection;