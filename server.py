#!/usr/bin/twistd -ny

import os
import json
import redis
from twisted.names import dns, server, client, cache
from twisted.application import service, internet
from twisted.internet import defer
from twisted.python import log

CONFIG_FILE = "config.json"
DOMAINS_KEY = "domains:"
RECORDS_KEY = "records:"


if __name__ == "__main__":
	import sys
	print "Usage: twistd -y %s" % sys.argv[0]


class DnsBroker(object):
	def __init__(self, backend):
		self.backend = backend

	def getDomainList(self):
		domains = self.backend.getSetMembers(DOMAINS_KEY)
		return domains

	def servesZone(self, zone):
		return self.backend.isSetMember(DOMAINS_KEY, zone)

	def getRecord(self, hostname):
		print "G> ?", RECORDS_KEY + hostname
		return self.backend.get(RECORDS_KEY + hostname)


class RedisBackend(object):
	def __init__(self, host, port, prefix = ""):
		self.host = host
		self.port = port
		self.prefix = prefix
		self.connect()
	
	def connect(self):
		self.conn = redis.StrictRedis(host = self.host, port = self.port)
	
	def completeKey(self, key):
		return self.prefix + key

	def get(self, key):
		key = self.completeKey(key)
		print "R> fetching ", key
		return self.conn.get(key)

	def set(self, key, value):
		key = self.completeKey(key)
		print "R> setting ", key, " to ", value
		return self.conn.set(key, value)

	def delete(self, key):
		key = self.completeKey(key)
		print "R> deleting ", key
		return self.conn.delete(key)

	def setAdd(self, key, value):
		key = self.completeKey(key)
		print "R> sadd ", key, ": ", value
		return self.conn.sadd(key, value)

	def pushRight(self, key, value):
		key = self.completeKey(key)
		print "R> rpushing ", key, " with ", value
		return self.conn.rpush(key, value)
	
	def getSetMembers(self, key):
		key = self.completeKey(key)
		print "R> smembers ", key
		return self.conn.smembers(key)

	def isSetMember(self, key, member):
		return self.conn.sismember(self.completeKey(key), member)


class BrokerResolver(client.Resolver):
	def __init__(self, broker, servers):
		self.broker = broker
		client.Resolver.__init__(self, servers = [("8.8.8.8", 53), ("8.8.4.4", 53)])
		self.ttl = 5

	def lookupAddress(self, name, timeout = None):
		print "X> lookupAddress ", name
		record = self.broker.getRecord(name)
		if record:
			print "X> record is ", record
			return [
					(dns.RRHeader(name, dns.A, dns.IN, self.ttl, dns.Record_A(record, self.ttl)), ), 
					(), 
					()
			]
		else:
			print "X> no record, forwarding"
			return self._lookup(name, dns.IN, dns.A, timeout).\
				addCallback(self.returnResult, True).\
				addErrback(self.returnResult, False)

	def returnResult(self, result, success):
		if success:
			return result

def bootstrap():
	if not os.path.exists(CONFIG_FILE):
		print CONFIG_FILE, "does not exist!"

	configFile = open(CONFIG_FILE, 'r')
	configJson = configFile.read()
	config = json.loads(configJson)

	app = service.Application('redisdns', 1, 1)

	backend = RedisBackend(host = config['redis']['host'], port = config['redis']['port'], prefix = config['redis']['prefix'])
	backend.setAdd(DOMAINS_KEY, "bc.fm")
	backend.setAdd(DOMAINS_KEY, "sys.fm")
	backend.setAdd(DOMAINS_KEY, "dfw9.sysnw.net")

	broker = DnsBroker(backend)
	resolver = BrokerResolver(broker, servers = config['upstream'])

	dnsFactory = server.DNSServerFactory(caches = [cache.CacheResolver()], clients = [resolver])
	dnsUdpFactory = dns.DNSDatagramProtocol(dnsFactory)
	dnsFactory.noisy = dnsUdpFactory.noisy = False
	
	serviceCollection = service.MultiService()
	serviceCollection.setServiceParent(service.IServiceCollection(app))
	
	internet.TCPServer(53, dnsFactory).setServiceParent(serviceCollection)
	internet.UDPServer(53, dnsUdpFactory).setServiceParent(serviceCollection)

	return app


application = bootstrap()

