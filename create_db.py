import sys, csv, sqlite3

con = sqlite3.connect('analytics.db') #creates the database if not yet existing
cur = con.cursor()
cur.executescript("""
	DROP TABLE IF EXISTS daily_aggregated_visitors;
	CREATE TABLE daily_aggregated_visitors (dates INTEGER, 
		channel TEXT, 
		device TEXT, 
		sessions INTEGER, 
		new_users INTEGER, 
		transactions INTEGER,
		revenue DOUBLE PRECISION);""")

with open('input1.csv','rb') as fin:
	dataread = csv.reader(fin, delimiter=',',)
	cur.executemany("INSERT INTO daily_aggregated_visitors VALUES(?,?,?,?,?,?,?)", dataread)
con.commit()

for row in cur.execute ('SELECT * FROM daily_aggregated_visitors LIMIT 10'):
	print row