import sqlite3
# import os
from fastapi import FastAPI, Query
from typing import List, Optional

app = FastAPI()


def get_db():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return conn


# @app.get('/user-stats')
# def user_stats(countries: Optional[List[str]] = Query(default=None), OSs: Optional[List[str]] = Query(default=None)):
#     conn = get_db()
#     cursor = conn.cursor()
#     query = '''
#         SELECT
#          username,
#          country,
#          (
#          SELECT map_name FROM maps JOIN matches ON maps.map_id = matches.map_id
#              JOIN match_outcomes ON matches.match_id = match_outcomes.match_id
#          WHERE match_outcomes.user_id = users.user_id
#          GROUP BY maps.map_id
#          ORDER BY COUNT(*) DESC
#          LIMIT 1
#          ) as fav_map,
#          (
#          SELECT ROUND(AVG(match_outcomes.outcome), 2) FROM maps JOIN matches ON maps.map_id = matches.map_id
#              JOIN match_outcomes ON matches.match_id = match_outcomes.match_id
#          WHERE match_outcomes.user_id = users.user_id
#          GROUP BY maps.map_id
#          ORDER BY COUNT(*) DESC
#          LIMIT 1
#          ) as fav_map_win_ratio,
#          (
#          SELECT SUM(duration) FROM sessions
#          WHERE sessions.user_id = users.user_id
#          ) as total_playtime,
#          ROUND(AVG(outcome), 2) as total_win_ratio,
#          ROUND(
#             (SELECT COUNT(*) FROM match_outcomes WHERE match_outcomes.user_id = users.user_id) * 1.0 /
#             (SELECT COUNT(*) FROM sessions WHERE sessions.user_id = users.user_id)
#             , 2) as avg_matches_per_session,
#          registration_date
#         FROM users LEFT JOIN sessions ON users.user_id = sessions.user_id
#             LEFT JOIN match_outcomes ON users.user_id = match_outcomes.user_id
#     '''
#
#     conditions = []
#     params = []
#
#     if countries:
#         placeholders = ','.join('?' * len(countries))
#         conditions.append(f'users.country IN ({placeholders})')
#         params.extend(countries)
#
#     if OSs:
#         placeholders = ','.join('?' * len(OSs))
#         conditions.append(f'sessions.device_os IN ({placeholders})')
#         params.extend(OSs)
#
#     if conditions:
#         query += ' WHERE ' + ' AND '.join(conditions)
#
#     query += ' GROUP BY users.user_id ORDER BY total_playtime DESC'
#
#     cursor.execute(query, params)
#     rows = cursor.fetchall()
#     conn.close()
#     return [dict(row) for row in rows]

@app.get('/user-stats')
def user_stats(countries: Optional[List[str]] = Query(default=None), OSs: Optional[List[str]] = Query(default=None)):
    conn = get_db()
    cursor = conn.cursor()

    conditions = []
    params = []

    if countries:
        placeholders = ','.join('?' * len(countries))
        conditions.append(f'users.country IN ({placeholders})')
        params.extend(countries)

    # build OS filter for subquery
    if OSs:
        os_placeholders = ','.join('?' * len(OSs))
        os_filter = f'AND sessions.device_os IN ({os_placeholders})'
    else:
        os_filter = ''

    where_clause = ''
    if conditions:
        where_clause = 'WHERE ' + ' AND '.join(conditions)

    query = f'''
        SELECT
            username,
            country,
            fav_map,
            fav_map_win_ratio,
            total_playtime,
            total_win_ratio,
            avg_matches_per_session,
            registration_date
        FROM (
            SELECT
                users.user_id,
                username,
                country,
                registration_date,
                (SELECT SUM(duration) FROM sessions
                 WHERE sessions.user_id = users.user_id
                 {os_filter}
                ) as total_playtime,
                ROUND(AVG(outcome), 2) as total_win_ratio,
                ROUND(
                    (SELECT COUNT(*) FROM match_outcomes WHERE match_outcomes.user_id = users.user_id) * 1.0 /
                    NULLIF((SELECT COUNT(*) FROM sessions WHERE sessions.user_id = users.user_id), 0)
                , 2) as avg_matches_per_session,
                (SELECT map_name FROM maps
                    JOIN matches ON maps.map_id = matches.map_id
                    JOIN match_outcomes mo ON matches.match_id = mo.match_id
                 WHERE mo.user_id = users.user_id
                 GROUP BY maps.map_id
                 ORDER BY AVG(mo.outcome) DESC
                 LIMIT 1
                ) as fav_map,
                (SELECT ROUND(AVG(mo.outcome), 2) FROM maps
                    JOIN matches ON maps.map_id = matches.map_id
                    JOIN match_outcomes mo ON matches.match_id = mo.match_id
                 WHERE mo.user_id = users.user_id
                 GROUP BY maps.map_id
                 ORDER BY AVG(mo.outcome) DESC
                 LIMIT 1
                ) as fav_map_win_ratio
            FROM users
            LEFT JOIN match_outcomes ON users.user_id = match_outcomes.user_id
            {where_clause}
            GROUP BY users.user_id
        )
        ORDER BY total_playtime DESC
    '''

    # params for OS filter in subquery need to be added too
    if OSs:
        # insert OS params after country params, before the rest
        final_params = OSs + params
    else:
        final_params = params

    cursor.execute(query, final_params)
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
