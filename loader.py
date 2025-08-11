#!/usr/bin/env python3

import os
import json
import mysql.connector
from datetime import datetime, date
import traceback
import glob
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "ipl_final"),
    "charset": os.getenv("DB_CHARSET", "utf8mb4"),
    "autocommit": os.getenv("DB_AUTOCOMMIT", "true").lower() == "true",
    "raise_on_warnings": os.getenv("DB_RAISE_ON_WARNINGS", "true").lower() == "true",
    "connection_timeout": int(os.getenv("DB_CONNECTION_TIMEOUT", 10))
}

JSON_FOLDER = r"C:/Users/nanda/Downloads/ipl_json" #Replace Me

def connect_no_db():
    cfg = DB_CONFIG.copy()
    cfg.pop("database", None)
    return mysql.connector.connect(**cfg)

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

def ensure_database_and_schema():
    print("Setting up database and schema...")
    try:
        conn = connect_no_db()
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_CONFIG['database']}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.commit()
        cur.close()
        conn.close()
        print("Database created/verified")
    except Exception as e:
        print(f"Failed to create database: {e}")
        raise

    conn = get_connection()
    cur = conn.cursor()

    table_creation_queries = [
        """
        CREATE TABLE IF NOT EXISTS `matches` (
            `match_id` VARCHAR(100) NOT NULL PRIMARY KEY,
            `start_date` DATE,
            `team_type` VARCHAR(50),
            `match_type` VARCHAR(50),
            `match_type_number` INT,
            `gender` VARCHAR(20),
            `competition` VARCHAR(255),
            `season` VARCHAR(50),
            `season_year` INT,
            `team1` VARCHAR(200),
            `team2` VARCHAR(200),
            `venue` VARCHAR(500),
            `city` VARCHAR(200),
            `country` VARCHAR(100) DEFAULT 'India',
            `toss_winner` VARCHAR(200),
            `toss_decision` VARCHAR(50),
            `result_type` VARCHAR(100),
            `result_method` VARCHAR(100),
            `winner` VARCHAR(200),
            `margin` VARCHAR(200),
            `target_runs` INT,
            `target_overs` DECIMAL(4,1),
            `overs` DECIMAL(4,1),
            `balls_per_over` INT DEFAULT 6,
            `json_version` VARCHAR(50),
            `data_version` VARCHAR(50),
            `player_of_match` TEXT,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY `idx_season` (`season_year`),
            KEY `idx_date` (`start_date`),
            KEY `idx_teams` (`team1`, `team2`),
            KEY `idx_venue` (`venue`),
            KEY `idx_winner` (`winner`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        """
        CREATE TABLE IF NOT EXISTS `players` (
            `player_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `player_name` VARCHAR(500) NOT NULL,
            `player_key` VARCHAR(500),
            `registry_id` VARCHAR(100),
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY `unique_name` (`player_name`),
            KEY `idx_player_key` (`player_key`),
            KEY `idx_registry_id` (`registry_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        """
        CREATE TABLE IF NOT EXISTS `match_players` (
            `match_id` VARCHAR(100) NOT NULL,
            `player_id` INT NOT NULL,
            `team` VARCHAR(200),
            `role` VARCHAR(100),
            `registry_name` VARCHAR(500),
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`match_id`, `player_id`),
            KEY `idx_team` (`team`),
            KEY `idx_role` (`role`),
            FOREIGN KEY (`player_id`) REFERENCES `players`(`player_id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        """
        CREATE TABLE IF NOT EXISTS `innings` (
            `innings_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `match_id` VARCHAR(100) NOT NULL,
            `innings_number` INT NOT NULL,
            `team` VARCHAR(200),
            `batting_team` VARCHAR(200),
            `bowling_team` VARCHAR(200),
            `total_runs` INT DEFAULT 0,
            `total_balls` INT DEFAULT 0,
            `wickets` INT DEFAULT 0,
            `declared` BOOLEAN DEFAULT FALSE,
            `forfeited` BOOLEAN DEFAULT FALSE,
            `overs` DECIMAL(5,2),
            `run_rate` DECIMAL(5,2),
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY `unique_innings` (`match_id`, `innings_number`),
            KEY `idx_team` (`team`),
            KEY `idx_batting_team` (`batting_team`),
            KEY `idx_bowling_team` (`bowling_team`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        """
        CREATE TABLE IF NOT EXISTS `deliveries` (
            `delivery_id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `match_id` VARCHAR(100) NOT NULL,
            `innings_number` INT NOT NULL,
            `over_number` INT NOT NULL,
            `ball_in_over` INT NOT NULL,
            `ball_sequence` INT,
            `batsman` VARCHAR(500),
            `batsman_id` INT,
            `non_striker` VARCHAR(500),
            `non_striker_id` INT,
            `bowler` VARCHAR(500),
            `bowler_id` INT,
            `runs_batsman` INT DEFAULT 0,
            `runs_extras` INT DEFAULT 0,
            `runs_total` INT DEFAULT 0,
            `extra_type` VARCHAR(50),
            `extra_value` INT DEFAULT 0,
            `is_wicket` BOOLEAN DEFAULT FALSE,
            `wicket_kind` VARCHAR(100),
            `wicket_player` VARCHAR(500),
            `wicket_player_id` INT,
            `fielder` VARCHAR(500),
            `fielder_id` INT,
            `review_type` VARCHAR(50),
            `review_by` VARCHAR(200),
            `review_umpire` VARCHAR(200),
            `review_batter` VARCHAR(500),
            `review_decision` VARCHAR(50),
            `replacements` TEXT,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            KEY `idx_match_innings` (`match_id`, `innings_number`),
            KEY `idx_over` (`match_id`, `innings_number`, `over_number`),
            KEY `idx_batsman` (`batsman_id`),
            KEY `idx_bowler` (`bowler_id`),
            KEY `idx_wicket_player` (`wicket_player_id`),
            KEY `idx_fielder` (`fielder_id`),
            KEY `idx_wicket` (`is_wicket`),
            KEY `idx_ball_sequence` (`match_id`, `innings_number`, `ball_sequence`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        """
        CREATE TABLE IF NOT EXISTS `partnerships` (
            `partnership_id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `match_id` VARCHAR(100) NOT NULL,
            `innings_number` INT NOT NULL,
            `wicket_number` INT,
            `batsman1` VARCHAR(500),
            `batsman1_id` INT,
            `batsman2` VARCHAR(500),
            `batsman2_id` INT,
            `runs` INT DEFAULT 0,
            `balls` INT DEFAULT 0,
            `minutes` INT,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            KEY `idx_match_innings` (`match_id`, `innings_number`),
            KEY `idx_batsman1` (`batsman1_id`),
            KEY `idx_batsman2` (`batsman2_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,

        """
        CREATE TABLE IF NOT EXISTS `match_officials` (
            `official_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `match_id` VARCHAR(100) NOT NULL,
            `role` VARCHAR(100) NOT NULL,
            `name` VARCHAR(500) NOT NULL,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            KEY `idx_match` (`match_id`),
            KEY `idx_role` (`role`),
            KEY `idx_name` (`name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    ]

    for i, query in enumerate(table_creation_queries, 1):
        try:
            cur.execute(query)
            conn.commit()
            print(f"Table {i}/{len(table_creation_queries)} created/verified")
        except Exception as e:
            print(f"Failed to create table {i}: {e}")
            conn.rollback()
            raise

    view_queries = [
        """
        CREATE OR REPLACE VIEW `match_summary` AS
        SELECT
            m.match_id,
            m.start_date,
            m.season_year,
            m.team1,
            m.team2,
            m.venue,
            m.city,
            m.winner,
            m.margin,
            m.player_of_match,
            COALESCE(i1.total_runs, 0) as team1_runs,
            COALESCE(i1.wickets, 0) as team1_wickets,
            COALESCE(i1.overs, 0) as team1_overs,
            COALESCE(i2.total_runs, 0) as team2_runs,
            COALESCE(i2.wickets, 0) as team2_wickets,
            COALESCE(i2.overs, 0) as team2_overs
        FROM matches m
        LEFT JOIN innings i1 ON m.match_id = i1.match_id AND i1.innings_number = 1
        LEFT JOIN innings i2 ON m.match_id = i2.match_id AND i2.innings_number = 2
        """,

        """
        CREATE OR REPLACE VIEW `team_stats` AS
        SELECT
            team,
            COUNT(*) as matches_played,
            SUM(CASE WHEN winner = team THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN winner != team AND winner IS NOT NULL THEN 1 ELSE 0 END) as losses,
            ROUND(SUM(CASE WHEN winner = team THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as win_percentage
        FROM (
            SELECT match_id, team1 as team, winner FROM matches WHERE team1 IS NOT NULL
            UNION ALL
            SELECT match_id, team2 as team, winner FROM matches WHERE team2 IS NOT NULL
        ) team_matches
        GROUP BY team
        """
    ]

    for i, query in enumerate(view_queries, 1):
        try:
            cur.execute(query)
            conn.commit()
            print(f"View {i}/{len(view_queries)} created")
        except Exception as e:
            print(f"Warning: Failed to create view {i}: {e}")

    cur.close()
    conn.close()
    print("Database schema setup complete")

def safe_int(val, default=None):
    if val is None or val == '':
        return default
    try:
        if isinstance(val, str):
            val = val.strip()
            if not val:
                return default
        return int(float(val))
    except:
        return default

def safe_float(val, default=None):
    if val is None or val == '':
        return default
    try:
        if isinstance(val, str):
            val = val.strip()
            if not val:
                return default
        return float(val)
    except:
        return default

def safe_date(val):
    if not val:
        return None
    val = str(val).strip()
    if not val:
        return None

    date_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m-%d-%Y"
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(val, fmt).date()
        except:
            continue
    return None

def get_or_create_player(cur, conn, player_name, registry_id=None):
    if not player_name or not player_name.strip():
        return None

    clean_name = player_name.strip()
    if len(clean_name) > 500:
        clean_name = clean_name[:500]

    try:
        cur.execute("""
            INSERT INTO players (player_name, registry_id) VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
            player_id=LAST_INSERT_ID(player_id),
            registry_id=COALESCE(VALUES(registry_id), registry_id)
        """, (clean_name, registry_id))
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        try:
            cur.execute("SELECT player_id FROM players WHERE player_name=%s", (clean_name,))
            r = cur.fetchone()
            if r:
                return r[0]
            return None
        except Exception as e2:
            print(f"Failed to create/find player '{clean_name}': {e2}")
            return None

def process_json_file(json_file_path, conn, cur):
    match_filename = os.path.basename(json_file_path)
    match_id = match_filename.replace('.json', '')
    print(f"Processing match {match_id}...")

    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            match_data = json.load(f)
    except Exception as e:
        print(f"Failed to read JSON file {json_file_path}: {e}")
        return False

    info = match_data.get('info', {})
    meta = match_data.get('meta', {})
    innings_list = match_data.get('innings', [])

    match_id_val = str(match_id)
    start_date = safe_date(info.get('dates', [None])[0] if info.get('dates') else None)
    venue = info.get('venue')
    city = info.get('city')
    gender = info.get('gender', 'male')
    match_type = info.get('match_type', 'T20')
    overs = safe_int(info.get('overs', 20))
    balls_per_over = safe_int(info.get('balls_per_over', 6))

    teams = info.get('teams', [])
    team1 = teams[0] if len(teams) > 0 else None
    team2 = teams[1] if len(teams) > 1 else None

    event_info = info.get('event', {})
    competition = event_info.get('name', 'Indian Premier League')
    match_number = safe_int(event_info.get('match_number'))
    season_year = safe_int(info.get('season'))

    toss_info = info.get('toss', {})
    toss_winner = toss_info.get('winner')
    toss_decision = toss_info.get('decision')

    outcome = info.get('outcome', {})
    winner = outcome.get('winner')

    margin = None
    if 'by' in outcome:
        by_info = outcome['by']
        if 'runs' in by_info:
            margin = f"{by_info['runs']} runs"
        elif 'wickets' in by_info:
            margin = f"{by_info['wickets']} wickets"

    player_of_match = ', '.join(info.get('player_of_match', [])) if info.get('player_of_match') else None

    data_version = meta.get('data_version')
    created = meta.get('created')
    revision = safe_int(meta.get('revision'))

    try:
        cur.execute("""
            INSERT INTO matches (
                match_id, start_date, team_type, match_type, match_type_number, gender,
                competition, season, season_year, team1, team2, venue, city,
                toss_winner, toss_decision, winner, margin,
                overs, balls_per_over, player_of_match, data_version
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            start_date=VALUES(start_date), team_type=VALUES(team_type),
            match_type=VALUES(match_type), match_type_number=VALUES(match_type_number),
            gender=VALUES(gender), competition=VALUES(competition), season=VALUES(season),
            season_year=VALUES(season_year), team1=VALUES(team1), team2=VALUES(team2),
            venue=VALUES(venue), city=VALUES(city),
            toss_winner=VALUES(toss_winner), toss_decision=VALUES(toss_decision),
            winner=VALUES(winner), margin=VALUES(margin),
            overs=VALUES(overs), balls_per_over=VALUES(balls_per_over),
            player_of_match=VALUES(player_of_match), data_version=VALUES(data_version)
        """, (
            match_id_val, start_date, info.get('team_type', 'club'), match_type, match_number, gender,
            competition, str(season_year) if season_year else None, season_year, team1, team2, venue, city,
            toss_winner, toss_decision, winner, margin,
            overs, balls_per_over, player_of_match, data_version
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert match {match_id}: {e}")
        return False

    registry = info.get('registry', {}).get('people', {})
    players_info = info.get('players', {})

    try:
        for team, player_list in players_info.items():
            for player_name in player_list:
                registry_id = registry.get(player_name)
                player_id = get_or_create_player(cur, conn, player_name, registry_id)
                if player_id:
                    try:
                        cur.execute("""
                            INSERT INTO match_players (match_id, player_id, team, registry_name)
                            VALUES (%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE team=VALUES(team), registry_name=VALUES(registry_name)
                        """, (match_id_val, player_id, team, registry_id))
                    except Exception as e:
                        print(f"Failed to link player {player_name}: {e}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to process players for {match_id}: {e}")

    try:
        officials = info.get('officials', {})
        for umpire in officials.get('umpires', []):
            cur.execute("""
                INSERT INTO match_officials (match_id, role, name) VALUES (%s,%s,%s)
            """, (match_id_val, 'umpire', umpire))
        for tv_umpire in officials.get('tv_umpires', []):
            cur.execute("""
                INSERT INTO match_officials (match_id, role, name) VALUES (%s,%s,%s)
            """, (match_id_val, 'tv_umpire', tv_umpire))
        for reserve_umpire in officials.get('reserve_umpires', []):
            cur.execute("""
                INSERT INTO match_officials (match_id, role, name) VALUES (%s,%s,%s)
            """, (match_id_val, 'reserve_umpire', reserve_umpire))
        for match_referee in officials.get('match_referees', []):
            cur.execute("""
                INSERT INTO match_officials (match_id, role, name) VALUES (%s,%s,%s)
            """, (match_id_val, 'match_referee', match_referee))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to process officials for {match_id}: {e}")

    innings_data = {}
    total_deliveries = 0
    delivery_errors = 0

    for innings_idx, innings in enumerate(innings_list, 1):
        innings_team = innings.get('team')
        overs_list = innings.get('overs', [])

        innings_data[innings_idx] = {
            'team': innings_team,
            'total_runs': 0,
            'total_balls': 0,
            'wickets': 0,
            'max_over': 0
        }

        if innings_idx == 1:
            batting_team = innings_team
            bowling_team = team2 if innings_team == team1 else team1
        else:
            batting_team = innings_team
            bowling_team = team1 if innings_team == team2 else team2

        innings_data[innings_idx]['batting_team'] = batting_team
        innings_data[innings_idx]['bowling_team'] = bowling_team

        ball_sequence = 1
        for over_data in overs_list:
            over_num = safe_int(over_data.get('over'))
            deliveries = over_data.get('deliveries', [])
            innings_data[innings_idx]['max_over'] = max(innings_data[innings_idx]['max_over'], over_num)

            for ball_idx, delivery in enumerate(deliveries, 1):
                try:
                    batsman_name = delivery.get('batter')
                    non_striker_name = delivery.get('non_striker')
                    bowler_name = delivery.get('bowler')

                    batsman_id = get_or_create_player(cur, conn, batsman_name, registry.get(batsman_name))
                    non_striker_id = get_or_create_player(cur, conn, non_striker_name, registry.get(non_striker_name))
                    bowler_id = get_or_create_player(cur, conn, bowler_name, registry.get(bowler_name))

                    for pid, pname, team in [(batsman_id, batsman_name, batting_team),
                                           (non_striker_id, non_striker_name, batting_team),
                                           (bowler_id, bowler_name, bowling_team)]:
                        if pid and pname:
                            try:
                                cur.execute("""
                                    INSERT IGNORE INTO match_players (match_id, player_id, team)
                                    VALUES (%s,%s,%s)
                                """, (match_id_val, pid, team))
                            except:
                                pass

                    runs_info = delivery.get('runs', {})
                    runs_batsman = safe_int(runs_info.get('batter', 0))
                    runs_extras = safe_int(runs_info.get('extras', 0))
                    runs_total = safe_int(runs_info.get('total', runs_batsman + runs_extras))

                    extras_info = delivery.get('extras', {})
                    extra_type = None
                    extra_value = 0
                    if extras_info:
                        for ext_type in ['wides', 'noballs', 'byes', 'legbyes', 'penalty']:
                            if ext_type in extras_info:
                                extra_type = ext_type
                                extra_value = safe_int(extras_info[ext_type])
                                break

                    wickets = delivery.get('wickets', [])
                    is_wicket = len(wickets) > 0
                    wicket_kind = None
                    wicket_player_name = None
                    wicket_player_id = None
                    fielder_name = None
                    fielder_id = None

                    if is_wicket:
                        wicket = wickets[0]
                        wicket_kind = wicket.get('kind')
                        wicket_player_name = wicket.get('player_out')
                        wicket_player_id = get_or_create_player(cur, conn, wicket_player_name, registry.get(wicket_player_name))

                        fielders = wicket.get('fielders', [])
                        if fielders:
                            fielder_info = fielders[0]
                            fielder_name = fielder_info.get('name')
                            fielder_id = get_or_create_player(cur, conn, fielder_name, registry.get(fielder_name))

                    innings_data[innings_idx]['total_runs'] += runs_total
                    innings_data[innings_idx]['total_balls'] += 1
                    if is_wicket:
                        innings_data[innings_idx]['wickets'] += len(wickets)

                    cur.execute("""
                        INSERT INTO deliveries (
                            match_id, innings_number, over_number, ball_in_over, ball_sequence,
                            batsman, batsman_id, non_striker, non_striker_id, bowler, bowler_id,
                            runs_batsman, runs_extras, runs_total, extra_type, extra_value,
                            is_wicket, wicket_kind, wicket_player, wicket_player_id, fielder, fielder_id
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        match_id_val, innings_idx, over_num, ball_idx, ball_sequence,
                        batsman_name, batsman_id, non_striker_name, non_striker_id, bowler_name, bowler_id,
                        runs_batsman, runs_extras, runs_total, extra_type, extra_value,
                        is_wicket, wicket_kind, wicket_player_name, wicket_player_id, fielder_name, fielder_id
                    ))

                    ball_sequence += 1
                    total_deliveries += 1

                except Exception as e:
                    delivery_errors += 1
                    if delivery_errors <= 3:
                        print(f"Delivery error: {e}")

    try:
        for innings_num, data in innings_data.items():
            balls_played = data['total_balls']
            complete_overs = balls_played // balls_per_over
            remaining_balls = balls_played % balls_per_over
            overs_played = complete_overs + (remaining_balls / 10.0) if remaining_balls > 0 else complete_overs
            run_rate = (data['total_runs'] / overs_played) if overs_played > 0 else 0

            cur.execute("""
                INSERT INTO innings (
                    match_id, innings_number, team, batting_team, bowling_team,
                    total_runs, total_balls, wickets, overs, run_rate
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                team=VALUES(team), batting_team=VALUES(batting_team), bowling_team=VALUES(bowling_team),
                total_runs=VALUES(total_runs), total_balls=VALUES(total_balls),
                wickets=VALUES(wickets), overs=VALUES(overs), run_rate=VALUES(run_rate)
            """, (
                match_id_val, innings_num, data['team'], data['batting_team'], data['bowling_team'],
                data['total_runs'], data['total_balls'], data['wickets'], overs_played, run_rate
            ))

        conn.commit()
        print(f"Processed {total_deliveries} deliveries across {len(innings_data)} innings")
        if delivery_errors > 0:
            print(f"Skipped {delivery_errors} problematic deliveries")

    except Exception as e:
        conn.rollback()
        print(f"Failed to process deliveries for {match_id}: {e}")
        print(f"Error details: {traceback.format_exc()}")
        return False

    return True

def main():
    print("Starting IPL JSON to MySQL import process...")

    try:
        ensure_database_and_schema()

        conn = get_connection()
        cur = conn.cursor()

        pattern = os.path.join(JSON_FOLDER, "*.json")
        json_files = glob.glob(pattern)

        if not json_files:
            print(f"No JSON files found in {JSON_FOLDER}")
            print(f"Looking for pattern: {pattern}")
            print(f"Expected files like: 980961.json, 1473504.json, etc.")
            return

        print(f"\nFound {len(json_files)} JSON files")
        for f in json_files[:5]:
            print(f" - {os.path.basename(f)}")
        if len(json_files) > 5:
            print(f"... and {len(json_files) - 5} more files")

        successful = 0
        failed = 0

        for i, json_file_path in enumerate(json_files, 1):
            print(f"\n[{i:3d}/{len(json_files)}] ", end="")
            try:
                if process_json_file(json_file_path, conn, cur):
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                print(f"Fatal error processing {os.path.basename(json_file_path)}: {e}")
                if failed <= 3:
                    print(f"Traceback: {traceback.format_exc()}")
                try:
                    conn.rollback()
                except:
                    pass

            if i % 25 == 0 or i == len(json_files):
                print(f"\nProgress: {i}/{len(json_files)} files processed ({successful} successful, {failed} failed)")

        cur.close()
        conn.close()

        print("\nImport process completed!")
        print(f"Final Statistics:")
        print(f"Successfully processed: {successful} matches")
        print(f"Failed to process: {failed} matches")
        if len(json_files) > 0:
            print(f"Success rate: {(successful / len(json_files) * 100):.1f}%")

        print("\nData verification and analysis:")
        verification_conn = get_connection()
        verification_cur = verification_conn.cursor()

        try:
            tables = ['matches', 'players', 'match_players', 'innings', 'deliveries', 'partnerships', 'match_officials']
            for table in tables:
                verification_cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = verification_cur.fetchone()[0]
                print(f"{table}: {count:,} records")

            print("\nSample data validation:")
            verification_cur.execute("""
                SELECT COUNT(*) FROM matches m
                WHERE m.team1 IS NOT NULL AND m.team2 IS NOT NULL
                AND EXISTS (SELECT 1 FROM deliveries d WHERE d.match_id = m.match_id)
            """)
            complete_matches = verification_cur.fetchone()[0]
            print(f"Complete matches (with teams and deliveries): {complete_matches}")

            verification_cur.execute("SELECT innings_number, COUNT(*) FROM innings GROUP BY innings_number")
            innings_counts = verification_cur.fetchall()
            print(f"Innings distribution: {dict(innings_counts)}")

            verification_cur.execute("SELECT COUNT(*) FROM deliveries WHERE is_wicket = 1")
            wicket_deliveries = verification_cur.fetchone()[0]
            print(f"Total wicket deliveries: {wicket_deliveries}")

            verification_cur.execute("SELECT COUNT(*) FROM deliveries WHERE runs_extras > 0")
            extra_deliveries = verification_cur.fetchone()[0]
            print(f"Deliveries with extras: {extra_deliveries}")

            if successful > 0:
                print(f"\nSample recent matches:")
                verification_cur.execute("""
                    SELECT match_id, start_date, team1, team2, winner, margin
                    FROM matches
                    WHERE start_date IS NOT NULL
                    ORDER BY start_date DESC
                    LIMIT 5
                """)
                recent_matches = verification_cur.fetchall()
                for match in recent_matches:
                    print(f"{match[0]}: {match[2]} vs {match[3]} on {match[1]} - Winner: {match[4] or 'TBD'}")

            verification_cur.execute("""
                SELECT DISTINCT team1 FROM matches WHERE team1 IS NOT NULL
                UNION
                SELECT DISTINCT team2 FROM matches WHERE team2 IS NOT NULL
                ORDER BY 1
            """)
            teams = [row[0] for row in verification_cur.fetchall()]
            print(f"\nTeams found: {', '.join(teams[:10])}")
            if len(teams) > 10:
                print(f"... and {len(teams) - 10} more teams")

            verification_cur.execute("""
                SELECT season_year, COUNT(*) as matches
                FROM matches
                WHERE season_year IS NOT NULL
                GROUP BY season_year
                ORDER BY season_year DESC
            """)
            seasons = verification_cur.fetchall()
            print(f"\nSeason distribution:")
            for season, match_count in seasons[:10]:
                print(f"{season}: {match_count} matches")

            print(f"\nTop run scorers (from ball-by-ball data):")
            verification_cur.execute("""
                SELECT batsman, SUM(runs_batsman) as total_runs, COUNT(*) as balls_faced
                FROM deliveries
                WHERE batsman IS NOT NULL
                GROUP BY batsman
                ORDER BY total_runs DESC
                LIMIT 10
            """)
            top_scorers = verification_cur.fetchall()
            for i, (player, runs, balls) in enumerate(top_scorers, 1):
                strike_rate = (runs * 100.0 / balls) if balls > 0 else 0
                print(f"{i:2d}. {player}: {runs} runs ({balls} balls, SR: {strike_rate:.1f})")

            print(f"\nTop wicket takers:")
            verification_cur.execute("""
                SELECT bowler, COUNT(*) as wickets, COUNT(DISTINCT match_id) as matches
                FROM deliveries
                WHERE is_wicket = 1 AND bowler IS NOT NULL
                AND wicket_kind NOT IN ('run out', 'retired out', 'retired hurt', 'timed out')
                GROUP BY bowler
                ORDER BY wickets DESC
                LIMIT 10
            """)
            top_bowlers = verification_cur.fetchall()
            for i, (bowler, wickets, matches) in enumerate(top_bowlers, 1):
                avg_wickets = wickets / matches if matches > 0 else 0
                print(f"{i:2d}. {bowler}: {wickets} wickets ({matches} matches, {avg_wickets:.1f} wkts/match)")

        except Exception as e:
            print(f"Verification error: {e}")
        finally:
            verification_cur.close()
            verification_conn.close()

        print(f"\nImport completed! Database '{DB_CONFIG['database']}' is ready for analysis.")
        print(f"You can now run queries like:")
        print(f"SELECT * FROM match_summary LIMIT 10;")
        print(f"SELECT * FROM team_stats ORDER BY win_percentage DESC;")
        print(f"SELECT player_name, COUNT(*) as matches FROM match_players JOIN players USING(player_id) GROUP BY player_name ORDER BY matches DESC LIMIT 10;")
        print(f"SELECT batsman, SUM(runs_batsman) as runs FROM deliveries GROUP BY batsman ORDER BY runs DESC LIMIT 10;")
        print(f"SELECT bowler, COUNT(*) as wickets FROM deliveries WHERE is_wicket=1 GROUP BY bowler ORDER BY wickets DESC LIMIT 10;")

    except KeyboardInterrupt:
        print("\nImport process interrupted by user")
    except Exception as e:
        print(f"\nFatal error in main process: {e}")
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    main()
