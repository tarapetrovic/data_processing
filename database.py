import sqlite3
from datetime import datetime

conn = sqlite3.connect('database.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()


def create_tables():

    cursor.execute('DROP TABLE IF EXISTS match_outcomes')
    cursor.execute('DROP TABLE IF EXISTS matches')
    cursor.execute('DROP TABLE IF EXISTS sessions')
    cursor.execute('DROP TABLE IF EXISTS users')
    cursor.execute('DROP TABLE IF EXISTS maps')

    cursor.execute('PRAGMA foreign_keys = ON;')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            country TEXT NOT NULL,
            device_os TEXT NOT NULL,
            registration_date TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS maps (
            map_id TEXT PRIMARY KEY,
            map_name TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            start_time INTEGER NOT NULL,
            end_time INTEGER NOT NULL,
            map_id TEXT NOT NULL,
            player1_id TEXT NOT NULL,
            player2_id TEXT NOT NULL,
            FOREIGN KEY (map_id) REFERENCES maps (map_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_outcomes (
            match_id INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            outcome REAL NOT NULL,
            PRIMARY KEY (match_id, user_id),
            FOREIGN KEY (match_id) REFERENCES matches (match_id),
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            session_id INTEGER PRIMARY KEY,
            user_id TEXT NOT NULL,
            start_time INTEGER NOT NULL,
            end_time INTEGER NOT NULL,
            duration INTEGER NOT NULL,
            device_os TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')

    conn.commit()


def insert_maps(maps):
    for map in maps:
        cursor.execute('''
            INSERT OR IGNORE INTO maps (map_id, map_name) VALUES (?, ?)
        ''', (
            map['id'],
            map['name']
        ))
    conn.commit()


def insert_users(users):
    for user in users:
        cursor.execute('''
            INSERT OR IGNORE INTO users (user_id, username, country, device_os, registration_date) VALUES (?, ?, ?, ?, ?)
        ''', (
            user['user_id'],
            user['event_data']['username'],
            user['event_data']['country'],
            user['event_data']['device_os'],
            datetime.utcfromtimestamp(user['timestamp']).strftime('%Y-%m-%d')
        ))
    conn.commit()


def insert_sessions(sessions):
    cursor.execute('''SELECT user_id FROM users''')
    registered_users = {row['user_id'] for row in cursor.fetchall()}

    for session in sessions:
        if session['user_id'] in registered_users:
            cursor.execute('''
                INSERT INTO sessions (session_id, user_id, start_time, end_time, duration, device_os) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                session['session_id'],
                session['user_id'],
                session['start_time'],
                session['end_time'],
                session['duration'],
                session['device_os']
            ))
    conn.commit()


def insert_matches(matches):
    cursor.execute('SELECT user_id FROM users')
    registered_users = {row['user_id'] for row in cursor.fetchall()}
    for match in matches:
        if match['player1_id'] in registered_users and match['player2_id'] in registered_users:
            cursor.execute('''
                INSERT INTO matches (match_id, start_time, end_time, map_id, player1_id, player2_id) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                match['match_id'],
                match['start_time'],
                match['end_time'],
                match['map_id'],
                match['player1_id'],
                match['player2_id']
            ))
            for user_id, outcome in match['outcomes'].items():
                cursor.execute('''
                    INSERT INTO match_outcomes (match_id, user_id, outcome) VALUES (?, ?, ?)
                ''', (
                    match['match_id'],
                    user_id,
                    outcome
                ))
    conn.commit()


def setup_database(maps, registrations, sessions, matches):
    create_tables()
    insert_maps(maps)
    insert_users(registrations)
    insert_sessions(sessions)
    insert_matches(matches)
    conn.close()

