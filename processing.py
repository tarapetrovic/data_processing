import json
from collections import defaultdict
from database import setup_database


def load_jsonl(filepath):
    lines = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    lines.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"Skipping badly formatted line: {line}")
    return lines


def clean_events(events):
    valid_events = []
    for event in events:
        if (event.get('id') is None or
                event.get('timestamp') is None or
                event.get('event_type') is None or
                event.get('user_id') is None or
                event.get('event_data') is None):
            #print(f"Skipping event with missing fields: {event}")
            continue

        if isinstance(event.get('id'), int) and event.get('timestamp') and isinstance(event.get('timestamp'),
                                                                                      int) and event.get(
            'user_id') and isinstance(event.get('user_id'), str):
            if (
                    (event.get('event_type') == 'registration' and is_valid_registration(event)) or
                    (event.get('event_type') == 'session_ping' and is_valid_session_ping(event)) or
                    (event.get('event_type') == 'match_start' and is_valid_match_start(event)) or
                    (event.get('event_type') == 'match_finish' and is_valid_match_finish(event))
            ):
                valid_events.append(event)

    # discarding duplicates
    unique_events = {}
    for event in valid_events:
        event_id = event['id']
        if event_id not in unique_events or event['timestamp'] < unique_events[event_id]['timestamp']:
            unique_events[event_id] = event

    return list(unique_events.values())


def is_valid_registration(event):
    data = event.get('event_data', {})
    return (
            data.get('country') and isinstance(data.get('country'), str) and
            data.get('device_os') in ('iOS', 'Android') and data.get('username') and
            isinstance(data.get('username'), str)
    )


def is_valid_session_ping(event):
    data = event.get('event_data', {})
    return (
            data.get('state') in ('started', 'in_progress', 'ended') and
            data.get('device_os') in ('iOS', 'Android')
    )


def is_valid_match_start(event):
    data = event.get('event_data', {})
    return (
            event.get('user_id') != data.get('opponent_id') and
            data.get('map_id') and isinstance(data.get('map_id'), str) and
            data.get('opponent_id') and isinstance(data.get('opponent_id'), str)
    )


def is_valid_match_finish(event):
    data = event.get('event_data', {})
    return (
            event.get('user_id') != data.get('opponent_id') and
            data.get('map_id') and isinstance(data.get('map_id'), str) and
            data.get('opponent_id') and isinstance(data.get('opponent_id'), str) and
            data.get('outcome') in (0, 0.5, 1.0)
    )


def build_sessions(session_pings):
    pings_by_users = defaultdict(list)
    for ping in session_pings:
        pings_by_users[ping['user_id']].append(ping)

    sessions = []
    session_id = 0

    for user_id, pings in pings_by_users.items():
        pings.sort(key=lambda x: x['timestamp'])

        start_session = pings[0]
        prev_timestamp = pings[0]['timestamp']

        for ping in pings[1:]:
            curr_timestamp = ping['timestamp']
            if curr_timestamp - prev_timestamp > 120:
                # close the current session
                sessions.append({
                    'session_id': session_id,
                    'user_id': user_id,
                    'start_time': start_session['timestamp'],
                    'end_time': prev_timestamp,
                    'duration': prev_timestamp - start_session['timestamp'],
                    'device_os': start_session['event_data']['device_os']
                })
                session_id += 1
                # start a new session
                start_session = ping
            prev_timestamp = curr_timestamp

        # close the last session for the user
        sessions.append({
            'session_id': session_id,
            'user_id': user_id,
            'start_time': start_session['timestamp'],
            'end_time': prev_timestamp,
            'duration': prev_timestamp - start_session['timestamp'],
            'device_os': start_session['event_data']['device_os']
        })
        session_id += 1

    return sessions


def build_matches(match_starts, match_finishes):
    match_events = match_starts + match_finishes
    match_events.sort(key=lambda x: x['timestamp'])

    ongoing_matches = {}
    finished_matches = []
    match_id = 0

    for event in match_events:
        if event['event_type'] == 'match_start':
            key = (frozenset([event['user_id'], event['event_data']['opponent_id']]), event['event_data']['map_id'])
            ongoing_matches[key] = {
                'match_id': None,
                'start_time': event['timestamp'],
                'end_time': None,
                'map_id': event['event_data']['map_id'],
                'player1_id': event['user_id'],
                'player2_id': event['event_data']['opponent_id'],
                'outcomes': {}
            }
        elif event['event_type'] == 'match_finish':
            key = (frozenset([event['user_id'], event['event_data']['opponent_id']]), event['event_data']['map_id'])
            if key in ongoing_matches:
                ongoing_matches[key]['match_id'] = match_id
                ongoing_matches[key]['end_time'] = event['timestamp']
                ongoing_matches[key]['outcomes'][event['user_id']] = event['event_data']['outcome']
                ongoing_matches[key]['outcomes'][event['event_data']['opponent_id']] = 1.0 - event['event_data']['outcome']

                finished_matches.append(ongoing_matches[key])
                del ongoing_matches[key]
                match_id += 1
    return finished_matches


def process():
    events = load_jsonl('data/events.jsonl')
    maps = load_jsonl('data/maps.jsonl')
    cleaned_events = clean_events(events)

    registrations = []
    session_pings = []
    match_starts = []
    match_finishes = []
    for e in cleaned_events:
        if e['event_type'] == 'registration':
            registrations.append(e)
        elif e['event_type'] == 'session_ping':
            session_pings.append(e)
        elif e['event_type'] == 'match_start':
            match_starts.append(e)
        elif e['event_type'] == 'match_finish':
            match_finishes.append(e)

    sessions = build_sessions(session_pings)
    finished_matches = build_matches(match_starts, match_finishes)

    # print(f"Registrations: {len(registrations)}")
    # print(f"Session pings: {len(session_pings)}")
    # print(f"Match starts: {len(match_starts)}")
    # print(f"Match finishes: {len(match_finishes)}")
    # print(f"Sessions: {len(sessions)}")
    # print(f"Matches: {len(finished_matches)}")
    # print(f"Maps: {len(maps)}")

    return registrations, maps, sessions, finished_matches


if __name__ == '__main__':
    registrations, maps, sessions, matches = process()
    setup_database(maps, registrations, sessions, matches)

