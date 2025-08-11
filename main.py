#!/usr/bin/env python3

import asyncio
import json
import logging
import mysql.connector
from typing import Dict, List, Any, Optional, Tuple, Union
import re
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import datetime as dt
from decimal import Decimal
from mcp.server.fastmcp import FastMCP

load_dotenv()

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("IPLMCP")

DB_CONFIG: Dict[str, Any] = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "ipl_final"),
    "charset": os.getenv("DB_CHARSET", "utf8mb4"),
    "autocommit": os.getenv("DB_AUTOCOMMIT", "true").lower() == "true",
    "raise_on_warnings": os.getenv("DB_RAISE_ON_WARNINGS", "true").lower() == "true",
    "connection_timeout": int(os.getenv("DB_CONNECTION_TIMEOUT", 10)),
}

SERVER_NAME = os.getenv('SERVER_NAME', 'IPLMCP')
SERVER_VERSION = os.getenv('SERVER_VERSION', '1.0.0')
DB_MAX_RETRIES = int(os.getenv('DB_MAX_RETRIES', 3))

class MySQLResultFormatter:
    @staticmethod
    def format_mysql_output(results: List[Dict], description: str, sql: str, execution_time: float = 0) -> str:
        if not results:
            return f"""-- {description}
-- Query: {sql.replace(chr(10), ' ').strip()}
-- Execution time: {execution_time:.3f}s

Empty set ({execution_time:.3f} sec)
"""

        columns = list(results[0].keys())
        col_widths = {}
        for col in columns:
            col_widths[col] = max(
                len(str(col)),
                max(len(str(row.get(col, ''))) for row in results),
                4
            )

        header_line = "+" + "+".join("-" * (col_widths[col] + 2) for col in columns) + "+"
        header_row = "|" + "|".join(f" {col:{col_widths[col]}} " for col in columns) + "|"
        
        data_rows = []
        for row in results:
            data_row = "|" + "|".join(f" {str(row.get(col, '')):{col_widths[col]}} " for col in columns) + "|"
            data_rows.append(data_row)

        output_lines = [
            f"-- {description}",
            f"-- Query: {sql.replace(chr(10), ' ').strip()}",
            f"-- Execution time: {execution_time:.3f}s",
            "",
            header_line,
            header_row,
            header_line,
            *data_rows,
            header_line,
            f"{len(results)} row{'s' if len(results) != 1 else ''} in set ({execution_time:.3f} sec)"
        ]

        return "\n".join(output_lines)

class EnhancedIPLDatabase:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.is_connected = False

    async def connect(self):
        for attempt in range(DB_MAX_RETRIES):
            try:
                self.connection = mysql.connector.connect(**self.config)
                self.is_connected = True
                logger.info("Database connection established")
                return
            except mysql.connector.Error as e:
                logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt == DB_MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(1)

    async def disconnect(self):
        if self.connection and self.is_connected:
            self.connection.close()
            self.is_connected = False
            logger.info("Database connection closed")

    async def execute_query(self, sql: str, params: List[Any] = None, description: str = "Query") -> str:
        if not self.is_connected:
            await self.connect()

        cursor = None
        start_time = datetime.now()
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(sql, params or [])
            results = cursor.fetchall()
            execution_time = (datetime.now() - start_time).total_seconds()

            for row in results:
                for key, value in row.items():
                    if isinstance(value, (dt.datetime, dt.date)):
                        row[key] = str(value)
                    elif isinstance(value, Decimal):
                        row[key] = float(value)

            return MySQLResultFormatter.format_mysql_output(
                results, description, sql, execution_time
            )

        except mysql.connector.Error as e:
            error_msg = f"""-- Query Error: {description}
-- SQL: {sql.replace(chr(10), ' ').strip()}
-- Parameters: {params or []}

ERROR {e.errno} ({e.sqlstate}): {e.msg}
"""
            logger.error(f"Database query failed: {e}")
            return error_msg

        except Exception as e:
            error_msg = f"""-- Query Error: {description}
-- SQL: {sql.replace(chr(10), ' ').strip()}

Error: {str(e)}
"""
            logger.error(f"Unexpected query execution error: {e}")
            return error_msg

        finally:
            if cursor:
                cursor.close()

class AdvancedIPLQueryProcessor:
    def __init__(self):
        self.query_patterns = {
            'recent_matches': [
                r'(?:show|get|list|display)\s+(?:recent|latest|last)\s+(?:\d+\s+)?matches?',
                r'(?:recent|latest|last)\s+(?:\d+\s+)?matches?',
                r'(?:last|recent)\s+(\d+)\s+matches?'
            ],
            'season_matches': [
                r'(?:show|get|list)\s+(?:all\s+)?matches?\s+(?:in|for|from|during)\s+(\d{4})(?:\s+season)?',
                r'(?:all\s+)?matches?\s+(?:in|for|from|during)\s+(\d{4})(?:\s+season)?',
                r'(?:ipl\s+)?(\d{4})\s+(?:season\s+)?matches?'
            ],
            'team_matches': [
                r'(?:show|get|list)\s+matches?\s+(?:for|of|by)\s+(.+?)(?:\s+team)?(?:\s+(?:in|during)\s+(\d{4}))?',
                r'(.+?)\s+(?:team\s+)?matches?(?:\s+(?:in|during)\s+(\d{4}))?',
                r'matches?\s+(?:played\s+)?(?:by|for)\s+(.+?)(?:\s+(?:in|during)\s+(\d{4}))?'
            ],
            'head_to_head': [
                r'(.+?)\s+(?:vs|v|against)\s+(.+?)\s+(?:head\s+to\s+head|h2h|record|matches?)',
                r'(?:head\s+to\s+head|h2h|record)\s+(?:between\s+)?(.+?)\s+(?:and|vs|v)\s+(.+?)',
                r'(?:match\s+)?(?:history|record)\s+(?:between\s+)?(.+?)\s+(?:and|vs|v)\s+(.+?)'
            ],
            'team_performance': [
                r'(?:team\s+)?(?:performance|statistics|stats|analysis)\s+(?:for|of)\s+(.+?)(?:\s+(?:in|during)\s+(\d{4}))?',
                r'(.+?)\s+(?:team\s+)?(?:performance|statistics|stats|analysis)(?:\s+(?:in|during)\s+(\d{4}))?',
                r'(?:win\s+)?(?:percentage|rate|ratio)\s+(?:for|of)\s+(.+?)(?:\s+(?:in|during)\s+(\d{4}))?'
            ],
            'batting_stats': [
                r'(?:top|best|highest)\s+(?:\d+\s+)?(?:run\s+)?scorers?(?:\s+(?:in|from|during)\s+(\d{4}|\w+))?',
                r'(?:batting\s+)?(?:statistics|stats|performance)\s+(?:of|for)\s+(.+?)(?:\s+(?:in|during)\s+(\d{4}))?',
                r'(.+?)\s+(?:batting\s+)?(?:statistics|stats|performance|record)(?:\s+(?:in|during)\s+(\d{4}))?'
            ],
            'bowling_stats': [
                r'(?:top|best|highest)\s+(?:\d+\s+)?(?:wicket\s+)?takers?(?:\s+(?:in|from|during)\s+(\d{4}|\w+))?',
                r'(?:bowling\s+)?(?:statistics|stats|performance)\s+(?:of|for)\s+(.+?)(?:\s+(?:in|during)\s+(\d{4}))?',
                r'(.+?)\s+(?:bowling\s+)?(?:statistics|stats|performance|record)(?:\s+(?:in|during)\s+(\d{4}))?'
            ],
            'match_scorecard': [
                r'(?:scorecard|score)\s+(?:for|of)\s+(.+?)(?:\s+vs?\s+(.+?))?(?:\s+(?:match|game))?(?:\s+(?:on|in)\s+(.+?))?',
                r'(?:show|get)\s+(?:match\s+)?(?:details?\s+)?(?:for\s+)?(.+?)(?:\s+vs?\s+(.+?))?(?:\s+(?:on|in)\s+(.+?))?'
            ],
            'venue_stats': [
                r'(?:matches\s+)?(?:at|in)\s+(.+?)(?:\s+(?:venue|ground|stadium|city))?(?:\s+(?:statistics|stats|analysis))?',
                r'(?:venue|ground|stadium)\s+(?:statistics|stats|analysis)(?:\s+(?:for|of)\s+(.+?))?'
            ],
            'season_summary': [
                r'(?:ipl\s+)?(\d{4})\s+(?:season\s+)?(?:statistics|stats|summary|analysis|winners?|champions?)',
                r'season\s+(\d{4})\s+(?:statistics|stats|summary|analysis)'
            ],
            'points_table': [
                r'(?:points?\s+table|standings|league\s+table)\s+(?:for\s+)?(\d{4})(?:\s+season)?',
                r'(?:final\s+)?(?:standings|table|positions?)\s+(?:for\s+)?(\d{4})?'
            ]
        }

        self.team_mappings = {
            'csk': 'Chennai Super Kings',
            'mi': 'Mumbai Indians',
            'rcb': 'Royal Challengers Bangalore',
            'kkr': 'Kolkata Knight Riders',
            'dc': 'Delhi Capitals',
            'rr': 'Rajasthan Royals',
            'pbks': 'Punjab Kings',
            'kxip': 'Punjab Kings',
            'srh': 'Sunrisers Hyderabad',
            'gt': 'Gujarat Titans',
            'lsg': 'Lucknow Super Giants',
            'dd': 'Delhi Capitals',
            'rps': 'Rising Pune Supergiant',
            'gl': 'Gujarat Lions',
            'ktk': 'Kochi Tuskers Kerala',
            'pwi': 'Pune Warriors India',
            'daredevils': 'Delhi Capitals',
            'kings xi punjab': 'Punjab Kings'
        }

    def normalize_team_name(self, team: str) -> str:
        if not team:
            return team
        team_lower = team.lower().strip()
        return self.team_mappings.get(team_lower, team)

    def identify_query_type(self, query: str) -> Tuple[str, Dict[str, Any]]:
        query_lower = query.lower().strip()
        
        for query_type, patterns in self.query_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, query_lower)
                if match:
                    params = self.extract_parameters(query_type, match.groups(), query_lower)
                    logger.info(f"Query type: {query_type}, Params: {params}")
                    return query_type, params
        
        return 'general_stats', {}

    def extract_parameters(self, query_type: str, groups: tuple, query_lower: str) -> Dict[str, Any]:
        params = {}
        
        # Extract limit from query
        limit_match = re.search(r'(?:top|first|last)\s+(\d+)', query_lower)
        if limit_match:
            params['limit'] = int(limit_match.group(1))
        else:
            params['limit'] = 20

        # Extract year from query
        year_match = re.search(r'\b(20\d{2})\b', query_lower)
        if year_match:
            params['year'] = year_match.group(1)

        if groups:
            if query_type in ['team_matches', 'team_performance', 'batting_stats', 'bowling_stats']:
                if groups[0]:
                    params['team_or_player'] = self.normalize_team_name(groups[0].strip())
                if len(groups) > 1 and groups[1]:
                    params['year'] = groups[1]
                    
            elif query_type in ['head_to_head', 'match_scorecard']:
                if groups[0]:
                    params['entity1'] = self.normalize_team_name(groups[0].strip())
                if len(groups) > 1 and groups[1]:
                    params['entity2'] = self.normalize_team_name(groups[1].strip())
                if len(groups) > 2 and groups:
                    if groups.isdigit():
                        params['year'] = groups
                    else:
                        params['venue_or_date'] = groups[2]
                        
            elif query_type in ['venue_stats']:
                if groups[0]:
                    params['venue_or_team'] = groups.strip()
                    
            elif query_type in ['season_summary', 'points_table']:
                if groups[0]:
                    params['year'] = groups

        return params

    def generate_sql(self, query_type: str, params: Dict[str, Any]) -> Tuple[str, List[Any]]:
        if query_type == 'recent_matches':
            limit = params.get('limit', 20)
            sql = """
            SELECT 
                m.match_id,
                DATE_FORMAT(m.start_date, '%Y-%m-%d') as match_date,
                m.team1,
                m.team2,
                m.winner,
                m.margin,
                m.venue,
                m.city,
                m.player_of_match,
                CONCAT(COALESCE(i1.total_runs, 0), '/', COALESCE(i1.wickets, 0)) as team1_score,
                CONCAT(COALESCE(i2.total_runs, 0), '/', COALESCE(i2.wickets, 0)) as team2_score
            FROM matches m
            LEFT JOIN innings i1 ON m.match_id = i1.match_id AND i1.innings_number = 1
            LEFT JOIN innings i2 ON m.match_id = i2.match_id AND i2.innings_number = 2
            WHERE m.start_date IS NOT NULL
            ORDER BY m.start_date DESC
            LIMIT %s
            """
            return sql, [limit]

        elif query_type == 'season_matches':
            year = params.get('year')
            if year:
                sql = """
                SELECT 
                    m.match_id,
                    DATE_FORMAT(m.start_date, '%Y-%m-%d') as match_date,
                    m.team1,
                    m.team2,
                    m.winner,
                    m.margin,
                    m.venue,
                    m.city,
                    CONCAT(COALESCE(i1.total_runs, 0), '/', COALESCE(i1.wickets, 0)) as team1_score,
                    CONCAT(COALESCE(i2.total_runs, 0), '/', COALESCE(i2.wickets, 0)) as team2_score
                FROM matches m
                LEFT JOIN innings i1 ON m.match_id = i1.match_id AND i1.innings_number = 1
                LEFT JOIN innings i2 ON m.match_id = i2.match_id AND i2.innings_number = 2
                WHERE m.season_year = %s
                ORDER BY m.start_date
                """
                return sql, [year]

        elif query_type == 'team_matches':
            team = params.get('team_or_player')
            year = params.get('year')
            if team:
                where_conditions = ["(LOWER(m.team1) LIKE LOWER(%s) OR LOWER(m.team2) LIKE LOWER(%s))"]
                sql_params = [f"%{team}%", f"%{team}%"]
                
                if year:
                    where_conditions.append("m.season_year = %s")
                    sql_params.append(year)

                sql = f"""
                SELECT 
                    m.match_id,
                    DATE_FORMAT(m.start_date, '%Y-%m-%d') as match_date,
                    m.team1,
                    m.team2,
                    m.winner,
                    m.margin,
                    m.venue,
                    CONCAT(COALESCE(i1.total_runs, 0), '/', COALESCE(i1.wickets, 0)) as team1_score,
                    CONCAT(COALESCE(i2.total_runs, 0), '/', COALESCE(i2.wickets, 0)) as team2_score
                FROM matches m
                LEFT JOIN innings i1 ON m.match_id = i1.match_id AND i1.innings_number = 1
                LEFT JOIN innings i2 ON m.match_id = i2.match_id AND i2.innings_number = 2
                WHERE {' AND '.join(where_conditions)}
                ORDER BY m.start_date DESC
                LIMIT 25
                """
                return sql, sql_params

        elif query_type == 'head_to_head':
            team1 = params.get('entity1')
            team2 = params.get('entity2')
            if team1 and team2:
                sql = """
                SELECT 
                    'Head-to-Head Analysis' as analysis_type,
                    CONCAT(%s, ' vs ', %s) as matchup,
                    COUNT(*) as total_matches,
                    SUM(CASE WHEN LOWER(winner) LIKE LOWER(%s) THEN 1 ELSE 0 END) as team1_wins,
                    SUM(CASE WHEN LOWER(winner) LIKE LOWER(%s) THEN 1 ELSE 0 END) as team2_wins,
                    COUNT(*) - SUM(CASE WHEN LOWER(winner) LIKE LOWER(%s) OR LOWER(winner) LIKE LOWER(%s) THEN 1 ELSE 0 END) as no_results,
                    ROUND(SUM(CASE WHEN LOWER(winner) LIKE LOWER(%s) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as team1_win_pct,
                    ROUND(SUM(CASE WHEN LOWER(winner) LIKE LOWER(%s) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as team2_win_pct
                FROM matches
                WHERE ((LOWER(team1) LIKE LOWER(%s) AND LOWER(team2) LIKE LOWER(%s))
                   OR (LOWER(team1) LIKE LOWER(%s) AND LOWER(team2) LIKE LOWER(%s)))
                """
                team1_pattern = f"%{team1}%"
                team2_pattern = f"%{team2}%"
                return sql, [team1, team2, team1_pattern, team2_pattern, team1_pattern, team2_pattern,
                           team1_pattern, team2_pattern, team1_pattern, team2_pattern, team2_pattern, team1_pattern]

        elif query_type == 'team_performance':
            team = params.get('team_or_player')
            year = params.get('year')
            if team:
                if year:
                    sql = """
                    SELECT 
                        t.team,
                        COUNT(DISTINCT CASE WHEN m.season_year = %s THEN m.match_id END) as matches_played,
                        SUM(CASE WHEN m.season_year = %s AND m.winner LIKE t.team THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN m.season_year = %s AND m.winner IS NOT NULL AND m.winner NOT LIKE t.team THEN 1 ELSE 0 END) as losses,
                        ROUND(SUM(CASE WHEN m.season_year = %s AND m.winner LIKE t.team THEN 1 ELSE 0 END) * 100.0 / 
                              NULLIF(COUNT(DISTINCT CASE WHEN m.season_year = %s THEN m.match_id END), 0), 2) as win_percentage
                    FROM (
                        SELECT DISTINCT team1 as team FROM matches WHERE LOWER(team1) LIKE LOWER(%s)
                        UNION
                        SELECT DISTINCT team2 as team FROM matches WHERE LOWER(team2) LIKE LOWER(%s)
                    ) t
                    LEFT JOIN matches m ON (LOWER(m.team1) LIKE LOWER(t.team) OR LOWER(m.team2) LIKE LOWER(t.team))
                    GROUP BY t.team
                    """
                    return sql, [year, year, year, year, year, f"%{team}%", f"%{team}%"]
                else:
                    sql = """
                    SELECT 
                        team,
                        matches_played,
                        wins,
                        losses,
                        ROUND(win_percentage, 2) as win_percentage
                    FROM team_stats
                    WHERE LOWER(team) LIKE LOWER(%s)
                    ORDER BY win_percentage DESC
                    """
                    return sql, [f"%{team}%"]
            else:
                year_condition = ""
                sql_params = []
                if year:
                    year_condition = f"WHERE season_year = %s"
                    sql_params = [year]

                sql = f"""
                SELECT 
                    team,
                    SUM(matches_played) as matches_played,
                    SUM(wins) as wins,
                    SUM(losses) as losses,
                    ROUND(SUM(wins) * 100.0 / NULLIF(SUM(matches_played), 0), 2) as win_percentage
                FROM (
                    SELECT 
                        team1 as team,
                        COUNT(*) as matches_played,
                        SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN winner != team1 AND winner IS NOT NULL THEN 1 ELSE 0 END) as losses
                    FROM matches m
                    {year_condition}
                    GROUP BY team1
                    UNION ALL
                    SELECT 
                        team2 as team,
                        COUNT(*) as matches_played,
                        SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN winner != team2 AND winner IS NOT NULL THEN 1 ELSE 0 END) as losses
                    FROM matches m
                    {year_condition}
                    GROUP BY team2
                ) team_stats
                GROUP BY team
                ORDER BY win_percentage DESC
                LIMIT 15
                """
                return sql, sql_params

        elif query_type == 'batting_stats':
            player = params.get('team_or_player')
            year = params.get('year')
            limit = params.get('limit', 15)
            
            where_conditions = ["d.batsman IS NOT NULL"]
            sql_params = []

            if player:
                where_conditions.append("LOWER(d.batsman) LIKE LOWER(%s)")
                sql_params.append(f"%{player}%")

            if year:
                where_conditions.append("m.season_year = %s")
                sql_params.append(year)
                join_matches = "JOIN matches m ON d.match_id = m.match_id"
            else:
                join_matches = ""

            sql = f"""
            SELECT 
                d.batsman as player,
                COUNT(DISTINCT d.match_id) as matches,
                SUM(d.runs_batsman) as total_runs,
                COUNT(*) as balls_faced,
                ROUND(SUM(d.runs_batsman) * 100.0 / COUNT(*), 2) as strike_rate,
                ROUND(SUM(d.runs_batsman) / COUNT(DISTINCT d.match_id), 2) as runs_per_match,
                SUM(CASE WHEN d.runs_batsman = 4 THEN 1 ELSE 0 END) as fours,
                SUM(CASE WHEN d.runs_batsman = 6 THEN 1 ELSE 0 END) as sixes,
                ROUND((SUM(CASE WHEN d.runs_batsman IN (4,6) THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) as boundary_percentage,
                ROUND((SUM(CASE WHEN d.runs_batsman = 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) as dot_ball_percentage
            FROM deliveries d
            {join_matches}
            WHERE {' AND '.join(where_conditions)}
            GROUP BY d.batsman
            HAVING matches >= 3
            ORDER BY total_runs DESC
            LIMIT %s
            """
            sql_params.append(limit)
            return sql, sql_params

        elif query_type == 'bowling_stats':
            player = params.get('team_or_player')
            year = params.get('year')
            limit = params.get('limit', 15)
            
            where_conditions = ["d.bowler IS NOT NULL"]
            sql_params = []

            if player:
                where_conditions.append("LOWER(d.bowler) LIKE LOWER(%s)")
                sql_params.append(f"%{player}%")

            if year:
                where_conditions.append("m.season_year = %s")
                sql_params.append(year)
                join_matches = "JOIN matches m ON d.match_id = m.match_id"
            else:
                join_matches = ""

            sql = f"""
            SELECT 
                d.bowler as player,
                COUNT(DISTINCT d.match_id) as matches,
                COUNT(*) as balls_bowled,
                ROUND(COUNT(*) / 6.0, 2) as overs_bowled,
                SUM(d.runs_total) as runs_conceded,
                COUNT(CASE WHEN d.is_wicket = 1 THEN 1 END) as wickets,
                ROUND(SUM(d.runs_total) * 6.0 / COUNT(*), 2) as economy_rate,
                ROUND(SUM(d.runs_total) / NULLIF(COUNT(CASE WHEN d.is_wicket = 1 THEN 1 END), 0), 2) as bowling_average,
                ROUND(COUNT(*) / NULLIF(COUNT(CASE WHEN d.is_wicket = 1 THEN 1 END), 0), 2) as strike_rate,
                ROUND((COUNT(CASE WHEN d.runs_total = 0 THEN 1 END) * 100.0) / COUNT(*), 2) as dot_ball_percentage
            FROM deliveries d
            {join_matches}
            WHERE {' AND '.join(where_conditions)}
            GROUP BY d.bowler
            HAVING matches >= 3
            ORDER BY wickets DESC
            LIMIT %s
            """
            sql_params.append(limit)
            return sql, sql_params

        elif query_type == 'match_scorecard':
            team1 = params.get('entity1')
            team2 = params.get('entity2')
            year = params.get('year')
            
            where_conditions = []
            sql_params = []

            if team1 and team2:
                where_conditions.append("((LOWER(m.team1) LIKE LOWER(%s) AND LOWER(m.team2) LIKE LOWER(%s)) OR (LOWER(m.team1) LIKE LOWER(%s) AND LOWER(m.team2) LIKE LOWER(%s)))")
                sql_params.extend([f"%{team1}%", f"%{team2}%", f"%{team2}%", f"%{team1}%"])
            elif team1:
                where_conditions.append("(LOWER(m.team1) LIKE LOWER(%s) OR LOWER(m.team2) LIKE LOWER(%s))")
                sql_params.extend([f"%{team1}%", f"%{team1}%"])

            if year:
                where_conditions.append("m.season_year = %s")
                sql_params.append(year)

            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            else:
                where_clause = ""

            sql = f"""
            SELECT 
                m.match_id,
                DATE_FORMAT(m.start_date, '%Y-%m-%d') as match_date,
                m.team1,
                m.team2,
                m.winner,
                m.margin,
                m.venue,
                m.city,
                m.player_of_match,
                CONCAT(COALESCE(i1.total_runs, 0), '/', COALESCE(i1.wickets, 0),
                       ' (', COALESCE(ROUND(i1.overs, 1), 0), ' overs)') as team1_score,
                CONCAT(COALESCE(i2.total_runs, 0), '/', COALESCE(i2.wickets, 0),
                       ' (', COALESCE(ROUND(i2.overs, 1), 0), ' overs)') as team2_score,
                ROUND(COALESCE(i1.run_rate, 0), 2) as team1_run_rate,
                ROUND(COALESCE(i2.run_rate, 0), 2) as team2_run_rate
            FROM matches m
            LEFT JOIN innings i1 ON m.match_id = i1.match_id AND i1.innings_number = 1
            LEFT JOIN innings i2 ON m.match_id = i2.match_id AND i2.innings_number = 2
            {where_clause}
            ORDER BY m.start_date DESC
            LIMIT 15
            """
            return sql, sql_params

        elif query_type == 'venue_stats':
            venue = params.get('venue_or_team')
            if venue:
                sql = """
                SELECT 
                    m.venue,
                    m.city,
                    COUNT(*) as matches_played,
                    ROUND(AVG(i1.total_runs + COALESCE(i2.total_runs, 0)), 2) as avg_total_runs,
                    MAX(i1.total_runs + COALESCE(i2.total_runs, 0)) as highest_total,
                    MIN(i1.total_runs + COALESCE(i2.total_runs, 0)) as lowest_total,
                    ROUND(AVG(CASE WHEN i1.total_runs > COALESCE(i2.total_runs, 0) THEN 1 ELSE 0 END) * 100, 2) as first_innings_win_pct,
                    COUNT(CASE WHEN m.margin LIKE '%wickets%' THEN 1 END) as chasing_wins,
                    COUNT(CASE WHEN m.margin LIKE '%runs%' THEN 1 END) as defending_wins
                FROM matches m
                LEFT JOIN innings i1 ON m.match_id = i1.match_id AND i1.innings_number = 1
                LEFT JOIN innings i2 ON m.match_id = i2.match_id AND i2.innings_number = 2
                WHERE LOWER(m.venue) LIKE LOWER(%s) OR LOWER(m.city) LIKE LOWER(%s)
                GROUP BY m.venue, m.city
                ORDER BY matches_played DESC
                """
                return sql, [f"%{venue}%", f"%{venue}%"]

        elif query_type == 'season_summary':
            year = params.get('year')
            if year:
                sql = """
                SELECT 
                    'Season Statistics' as category,
                    season_year as season,
                    COUNT(*) as total_matches,
                    COUNT(DISTINCT team1) + COUNT(DISTINCT team2) - COUNT(DISTINCT COALESCE(team1, team2)) as total_teams,
                    COUNT(DISTINCT venue) as venues_used,
                    winner as champion,
                    COUNT(CASE WHEN winner IS NOT NULL THEN 1 END) as completed_matches,
                    ROUND(AVG(COALESCE(i1.total_runs, 0) + COALESCE(i2.total_runs, 0)), 2) as avg_match_runs
                FROM matches m
                LEFT JOIN innings i1 ON m.match_id = i1.match_id AND i1.innings_number = 1
                LEFT JOIN innings i2 ON m.match_id = i2.match_id AND i2.innings_number = 2
                WHERE season_year = %s
                GROUP BY season_year, winner
                ORDER BY total_matches DESC
                LIMIT 1
                """
                return sql, [year]

        elif query_type == 'points_table':
            year = params.get('year', '2023')
            sql = """
            SELECT 
                team,
                matches_played,
                wins,
                losses,
                ROUND(win_percentage, 2) as win_percentage,
                CASE 
                    WHEN wins >= matches_played * 0.6 THEN 'Playoffs'
                    WHEN wins >= matches_played * 0.4 THEN 'Mid-table'
                    ELSE 'Bottom'
                END as position_category
            FROM (
                SELECT 
                    team,
                    SUM(matches_played) as matches_played,
                    SUM(wins) as wins,
                    SUM(losses) as losses,
                    ROUND(SUM(wins) * 100.0 / NULLIF(SUM(matches_played), 0), 2) as win_percentage
                FROM (
                    SELECT 
                        team1 as team,
                        COUNT(*) as matches_played,
                        SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN winner != team1 AND winner IS NOT NULL THEN 1 ELSE 0 END) as losses
                    FROM matches
                    WHERE season_year = %s
                    GROUP BY team1
                    UNION ALL
                    SELECT 
                        team2 as team,
                        COUNT(*) as matches_played,
                        SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN winner != team2 AND winner IS NOT NULL THEN 1 ELSE 0 END) as losses
                    FROM matches
                    WHERE season_year = %s
                    GROUP BY team2
                ) team_stats
                GROUP BY team
            ) final_table
            ORDER BY win_percentage DESC, wins DESC
            """
            return sql, [year, year]

        else:
            # general_stats
            sql = """
            SELECT 
                'General Statistics' as category,
                COUNT(DISTINCT season_year) as total_seasons,
                COUNT(*) as total_matches,
                COUNT(DISTINCT venue) as venues_used,
                COUNT(DISTINCT CONCAT(team1, '|', team2)) as unique_matchups
            FROM matches
            WHERE season_year IS NOT NULL
            """
            return sql, []

# Initialize components
query_processor = AdvancedIPLQueryProcessor()
database = EnhancedIPLDatabase(DB_CONFIG)
mcp = FastMCP(SERVER_NAME)

@mcp.tool()
async def query_ipl_cricket_data(query: str) -> str:
    """
    Query IPL cricket database using natural language.
    
    Args:
        query: Natural language query about IPL cricket data
        
    Returns:
        Formatted query results
    """
    if not query or not query.strip():
        return "ERROR: Please provide a valid cricket query."

    try:
        query_type, params = query_processor.identify_query_type(query)
        sql, sql_params = query_processor.generate_sql(query_type, params)
        description = f"{query_type.replace('_', ' ').title()}: {query}"
        
        logger.info(f"Processing: '{query}' -> {query_type} with params: {params}")
        result = await database.execute_query(sql, sql_params, description)
        return result

    except Exception as e:
        logger.error(f"Error processing query '{query}': {e}")
        return f"""-- IPLMCP Query Error
-- Query: {query}
-- Error: {str(e)}
-- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

ERROR: Failed to process query. Please check syntax and try again.
"""

@mcp.tool()
async def execute_direct_sql_query(sql_query: str) -> str:
    """
    Execute a direct SQL query on the IPL database.
    
    Args:
        sql_query: SQL query to execute (SELECT, SHOW, DESCRIBE only)
        
    Returns:
        Query results in formatted table
    """
    if not sql_query or not sql_query.strip():
        return "ERROR: Please provide a valid SQL query."

    sql_clean = sql_query.strip().upper()
    allowed_statements = ['SELECT', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN']
    
    if not any(sql_clean.startswith(stmt) for stmt in allowed_statements):
        return f"""-- SQL Security Error
-- Only {', '.join(allowed_statements)} statements are allowed
-- Provided: {sql_query[:50]}...

ERROR: Security restriction - only read-only queries permitted.
"""

    try:
        logger.info(f"Executing direct SQL: {sql_query[:100]}...")
        result = await database.execute_query(sql_query, [], "Direct SQL Query")
        return result

    except Exception as e:
        logger.error(f"Direct SQL execution error: {e}")
        return f"""-- SQL Execution Error
-- Query: {sql_query}
-- Error: {str(e)}
-- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

ERROR: Query execution failed. Check syntax and table names.
"""

@mcp.tool()
async def get_database_schema_info() -> str:
    """
    Get comprehensive information about the IPL database schema.
    
    Returns:
        Database schema information and usage guide
    """
    try:
        # Get table information
        tables_sql = """
        SELECT 
            table_name,
            table_rows,
            round(((data_length + index_length) / 1024 / 1024), 2) as size_mb,
            table_comment
        FROM information_schema.tables 
        WHERE table_schema = %s 
        AND table_type = 'BASE TABLE'
        ORDER BY table_rows DESC
        """
        tables_result = await database.execute_query(
            tables_sql, 
            [DB_CONFIG['database']], 
            "Database Tables Overview"
        )

        # Get column information for key tables
        columns_sql = """
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable,
            column_key,
            column_default,
            extra
        FROM information_schema.columns 
        WHERE table_schema = %s
        AND table_name IN ('matches', 'players', 'deliveries', 'innings')
        ORDER BY table_name, ordinal_position
        """
        columns_result = await database.execute_query(
            columns_sql, 
            [DB_CONFIG['database']], 
            "Key Table Column Definitions"
        )

        # Get database statistics
        stats_sql = """
        SELECT 
            'Total Matches' as metric,
            COUNT(*) as value,
            CONCAT('Seasons: ', MIN(season_year), '-', MAX(season_year)) as details
        FROM matches
        WHERE season_year IS NOT NULL
        UNION ALL
        SELECT 
            'Total Deliveries' as metric,
            COUNT(*) as value,
            CONCAT('Wickets: ', SUM(CASE WHEN is_wicket=1 THEN 1 ELSE 0 END)) as details
        FROM deliveries
        UNION ALL
        SELECT 
            'Unique Players' as metric,
            COUNT(*) as value,
            'All-time participants' as details
        FROM players
        UNION ALL
        SELECT 
            'Active Teams' as metric,
            COUNT(DISTINCT team) as value,
            'Current franchises' as details
        FROM team_stats
        """
        stats_result = await database.execute_query(
            stats_sql, 
            [], 
            "Database Statistics Summary"
        )

        schema_info = f"""-- IPL CRICKET DATABASE SCHEMA GUIDE
-- Database: {DB_CONFIG['database']}
-- Server: {DB_CONFIG['host']}:{DB_CONFIG['port']}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{stats_result}

{tables_result}

{columns_result}

-- SAMPLE NATURAL LANGUAGE QUERIES
-- Match Analysis:
--   "Show recent matches"
--   "Matches between CSK and MI in 2023"
--   "Season summary for 2022"

-- Player Performance:
--   "Top run scorers in IPL 2023"
--   "Virat Kohli batting statistics"
--   "Economy rate of Jasprit Bumrah"

-- Team Analytics:
--   "Team statistics for Mumbai Indians"
--   "Points table for 2023"
--   "Win percentage of all teams"

-- Advanced Analytics:
--   "Venue statistics for Wankhede Stadium"
--   "Boundary percentage for top batsmen"

-- SAMPLE DIRECT SQL QUERIES

SELECT m.team1, m.team2, m.winner, m.margin, m.venue,
       DATE_FORMAT(m.start_date, '%Y-%m-%d') as match_date
FROM matches m
ORDER BY m.start_date DESC LIMIT 10;

SELECT d.batsman, SUM(d.runs_batsman) as total_runs,
       COUNT(DISTINCT d.match_id) as matches,
       ROUND(SUM(d.runs_batsman)/COUNT(DISTINCT d.match_id), 2) as avg_per_match
FROM deliveries d
WHERE d.batsman IS NOT NULL
GROUP BY d.batsman
ORDER BY total_runs DESC LIMIT 15;

SELECT team, matches_played, wins,
       ROUND(win_percentage, 2) as win_pct
FROM team_stats
ORDER BY win_percentage DESC;

-- KEY RELATIONSHIPS
-- matches.match_id -> deliveries.match_id
-- matches.match_id -> innings.match_id
-- matches.match_id -> match_players.match_id
-- players.player_id -> match_players.player_id
-- deliveries: batsman_id, bowler_id, wicket_player_id -> players.player_id
"""

        return schema_info

    except Exception as e:
        logger.error(f"Error getting schema info: {e}")
        return f"""-- Schema Error
-- Database: {DB_CONFIG['database']}
-- Error: {str(e)}
-- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

ERROR: Unable to retrieve database schema information.
"""

if __name__ == "__main__":
    import asyncio
    mcp.run()
