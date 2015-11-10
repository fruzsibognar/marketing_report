"""   queries with the input files  """

import sqlite3 as lite


con = lite.connect('analytics.db')

dates_threshold = [ 20151010, 20151011, 20151012]
channel_threshold = [ 'Generic Paid Search', 'Branded Paid Search', 'Organic Search' ]
device_threshold = ['desktop', 'mobile']

def generate_query(channel, device, dates):
	return """SELECT dates, channel, device, 
    			SUM(sessions), SUM(new_users), SUM(transactions), SUM(revenue) 
    			FROM daily_aggregated_visitors
    			WHERE channel IN %s AND device IN %s AND dates IN %s
    			GROUP BY dates, channel, device """ % (('("' + '", "'.join(channel)  + '")'),('("' + '", "'.join(device)  + '")'), ('(' + ', '.join(str(d) for d in dates)  + ')'))


query_text = generate_query(channel_threshold, device_threshold, dates_threshold)

print query_text 

with con:    
    
    cur = con.cursor()    
    cur.execute(query_text)

    rows = cur.fetchall()

    for row in rows:
        print row
