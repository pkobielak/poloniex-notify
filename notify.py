import MySQLdb
import pynotify

from warnings import filterwarnings
filterwarnings('ignore', category = MySQLdb.Warning)


def send(title, message):
	pynotify.init("test")
	notice = pynotify.Notification(title, message)
	notice.show()
	return

class SQL:
	def __init__(self):
		self.db = MySQLdb.connect("localhost","root",
				"","trade")
		self.cursor = self.db.cursor()

		self.ask_for_coin_names = "SHOW TABLES"

	def get_last15min_prices(self, pair_name):
		self.last15_changes = "SELECT last FROM %s WHERE time >= "\
					"DATE_SUB(NOW(), INTERVAL 15 MINUTE)" % pair_name
		self.cursor.execute(self.last15_changes)
		return sql.cursor.fetchall()

sql = SQL()
sql.cursor.execute(sql.ask_for_coin_names)

results = sql.cursor.fetchall()



for pair in results:
	pair_name = pair[0]
	prices = sql.get_last15min_prices(pair_name)

	if prices:
		print "%-12s" % pair_name,
		oldest = prices[0][0]
		newest = prices[-1][0]
		change = oldest-newest
		percent_change = change/oldest*100
		str =  "Change: "+'{0:.2f}'.format(percent_change)+ "%"
		print str
##		if percent_change > 5 or percent_change < -5:
##			send(pair_name, str)

