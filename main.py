import sqlite3
# import os
from fastapi import FastAPI, Query
from typing import List, Optional

app = FastAPI()


def get_db():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return conn


@app.get('/user-stats')
def user_stats(countries: Optional[List[str]] = Query(default=None), OSs: Optional[List[str]] = Query(default=None)):
    conn = get_db()
    cursor = conn.cursor()
    query = '''
        SELECT
         username, 
         country, 
         (
         SELECT map_name FROM maps JOIN matches ON maps.map_id = matches.map_id 
             JOIN match_outcomes ON matches.match_id = match_outcomes.match_id 
         WHERE match_outcomes.user_id = users.user_id
         GROUP BY maps.map_id 
         ORDER BY COUNT(*) DESC 
         LIMIT 1
         ) as fav_map,
         (
         SELECT ROUND(AVG(match_outcomes.outcome), 2) FROM maps JOIN matches ON maps.map_id = matches.map_id 
             JOIN match_outcomes ON matches.match_id = match_outcomes.match_id 
         WHERE match_outcomes.user_id = users.user_id
         GROUP BY maps.map_id 
         ORDER BY COUNT(*) DESC 
         LIMIT 1
         ) as fav_map_win_ratio,
         SUM(duration) as total_playtime, 
         ROUND(AVG(outcome), 2) as total_win_ratio,
         ROUND(
            (SELECT COUNT(*) FROM match_outcomes WHERE match_outcomes.user_id = users.user_id) * 1.0 /
            (SELECT COUNT(*) FROM sessions WHERE sessions.user_id = users.user_id)
            , 2) as avg_matches_per_session,
         registration_date
        FROM users LEFT JOIN sessions ON users.user_id = sessions.user_id 
            LEFT JOIN match_outcomes ON users.user_id = match_outcomes.user_id
    '''

    conditions = []
    params = []

    if countries:
        placeholders = ','.join('?' * len(countries))
        conditions.append(f'users.country IN ({placeholders})')
        params.extend(countries)

    if OSs:
        placeholders = ','.join('?' * len(OSs))
        conditions.append(f'sessions.device_os IN ({placeholders})')
        params.extend(OSs)

    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)

    query += ' GROUP BY users.user_id ORDER BY total_playtime DESC'

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get('/map-stats/{map_name}')
def map_stats(map_name: str, date_from: Optional[str] = None, date_to: Optional[str] = None):
    conn = get_db()
    cursor = conn.cursor()

    conditions = ['maps.map_name = ?']
    params = [map_name]

    if date_from:
        conditions.append('DATE(m.end_time, "unixepoch") >= ?')
        params.append(date_from)

    if date_to:
        conditions.append('DATE(m.end_time, "unixepoch") <= ?')
        params.append(date_to)

    query = f'''
        SELECT 
           DATE(m.end_time, 'unixepoch') as date, 
           ROUND(AVG(m.end_time - m.start_time), 2) as avg_playtime, 
           (SELECT users.username
             FROM users JOIN match_outcomes ON users.user_id = match_outcomes.user_id
                 JOIN matches ON match_outcomes.match_id = matches.match_id
             WHERE matches.map_id = m.map_id
                 AND DATE(matches.end_time, 'unixepoch') <= DATE(m.end_time, 'unixepoch')
             GROUP BY match_outcomes.user_id
             ORDER BY AVG(match_outcomes.outcome) DESC
             LIMIT 1
           ) as best_player_username,
           COUNT(*) as match_cnt
        FROM matches m JOIN maps ON m.map_id = maps.map_id
        WHERE {' AND '.join(conditions)}
        GROUP BY date
        ORDER BY date DESC
    '''

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]
