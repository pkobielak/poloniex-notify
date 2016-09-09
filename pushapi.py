from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
import MySQLdb
from warnings import filterwarnings
filterwarnings('ignore', category = MySQLdb.Warning)

class PushReactor(ApplicationSession):
	@inlineCallbacks
	def onJoin(self, details):
		print("subscribed")
		self.processedTicks = 0
		yield self.subscribe(self.onTick, u'ticker')

	def onTick(self, *args):
		currencyPair = args[0]

		if "BTC" not in currencyPair:
			return

		last = args[1]
		lowestAsk = args[2]
		highestBid = args[3]
		percentChange = args[4]
		baseVolume = args[5]
		quoteVolume = args[6]
		isFrozen = args[7]
		_24hrHigh = args[8]
		_24hrLow = args[9]
		self.processedTicks += 1
		text =  "%s %s %s %s %s %s %s %s %s %s" % (currencyPair, last, lowestAsk, highestBid, percentChange, baseVolume, quoteVolume, isFrozen, _24hrHigh, _24hrLow)
		print  " Processed updates: ",self.processedTicks," \n",


		manager = ManageDB()
		manager.addRecord(currencyPair,last,lowestAsk,
				highestBid,percentChange,baseVolume,
				quoteVolume,isFrozen,_24hrHigh,
				_24hrLow)

class ManageDB():
	def __init__(self):
		self.db = MySQLdb.connect("localhost","root",
				"","trade")
		self.cursor = self.db.cursor()

	def addRecord(self, currencyPair, last, lowestAsk, highestBid,
		percentChange, baseVolume, quoteVolume,
		isFrozen, _24hrHigh, _24hrLow):

		sql_table = """CREATE TABLE IF NOT EXISTS %s
				(
				last FLOAT,
				lowestAsk FLOAT,
				highestBid FLOAT,
				percentChange FLOAT,
				baseVolume FLOAT,
				quoteVolume FLOAT,
				isFrozen INT,
				hrHigh FLOAT,
				hrLow FLOAT,
				time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""% (currencyPair)

		self.cursor.execute(sql_table)


		sql_insert = """INSERT INTO %s (last, lowestAsk,
				highestBid, percentChange, baseVolume,
				quoteVolume, isFrozen, hrHigh,
				hrLow) VALUES (%s,%s,%s,%s,%s,%s,
				%s,%s,%s)""" % (currencyPair,last,
				lowestAsk,highestBid,percentChange,
				baseVolume,quoteVolume,isFrozen,
				_24hrHigh,_24hrLow)

		self.cursor.execute(sql_insert)
		self.db.commit()



if __name__ == '__main__':
	runner = ApplicationRunner(u'wss://api.poloniex.com',u'realm1')
	runner.run(PushReactor)
