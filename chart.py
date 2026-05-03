import sqlite3
import matplotlib.pyplot as plt
from collections import defaultdict


conn = sqlite3.connect('database.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT DATE(MAX(end_time), 'unixepoch') FROM matches")
max_date = cursor.fetchone()[0]

cursor.execute('''
    SELECT DATE(m.end_time, 'unixepoch') as date,
           maps.map_name,
           COUNT(*) as match_cnt
    FROM matches m JOIN maps ON m.map_id = maps.map_id
    WHERE DATE(m.end_time, 'unixepoch') >= DATE(?, '-7 days')
    GROUP BY date, maps.map_name
    ORDER BY date
''', (max_date,))

rows = [dict(row) for row in cursor.fetchall()]
conn.close()

#print(rows)

# organize data by map
map_data = defaultdict(lambda: {'dates': [], 'counts': []})
for row in rows:
    map_data[row['map_name']]['dates'].append(row['date'])
    map_data[row['map_name']]['counts'].append(row['match_cnt'])

# plot
plt.figure(figsize=(10, 6))
for map_name, data in map_data.items():
    plt.plot(data['dates'], data['counts'], marker='o', label=map_name)

plt.title('Match Count per Map - Last 7 Days')
plt.xlabel('Date')
plt.ylabel('Match Count')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('match_chart.png')
plt.show()
