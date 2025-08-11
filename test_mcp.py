#!/usr/bin/env python3

import asyncio
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import traceback

load_dotenv()
sys.path.insert(0, str(Path.cwd()))

def check_environment():
    print("Checking Environment Configuration")
    print("=" * 40)
    
    env_file = Path(".env")
    if not env_file.exists():
        print(".env file not found")
        print("Please create .env file from .env.example")
        return False

    print(".env file exists")

    required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            display_value = "***" if "PASSWORD" in var else value
            print(f"{var}: {display_value}")

    if missing_vars:
        print(f"Missing required variables: {', '.join(missing_vars)}")
        print("Please update your .env file")
        return False

    return True

def check_dependencies():
    print("\nChecking Dependencies")
    print("=" * 25)
    
    dependencies = [
        ('mysql.connector', 'mysql-connector-python'),
        ('dotenv', 'python-dotenv'),
        ('mcp', 'mcp')
    ]

    missing_deps = []
    for module, package in dependencies:
        try:
            __import__(module)
            print(f"{package}")
        except ImportError:
            print(f"{package}")
            missing_deps.append(package)

    if missing_deps:
        print(f"\nMissing dependencies: {', '.join(missing_deps)}")
        print("Install with: uv add " + " ".join(missing_deps))
        return False

    return True

async def test_database_connection():
    print("\nTesting Database Connection")
    print("=" * 30)
    
    try:
        from main import database
        await database.connect()
        print("Database connection successful")

        results = await database.execute_query("SELECT 1 as test", [], "Test Query")
        if "1 row" in results:
            print("Database query execution working")
        else:
            print("Query executed but format unexpected")

        await database.disconnect()
        return True

    except ImportError as e:
        print(f"Import error: {e}")
        return False
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

async def test_database_schema():
    print("\nTesting Database Schema")
    print("=" * 30)
    
    try:
        from main import database
        await database.connect()

        required_tables = [
            'matches', 'players', 'innings', 'deliveries',
            'match_players', 'match_officials'
        ]

        table_status = {}
        for table in required_tables:
            try:
                result = await database.execute_query(f"SELECT COUNT(*) as count FROM {table}", [], f"Count {table}")
                
                if "Empty set" in result:
                    count = 0
                elif "|" in result and "count" in result:
                    lines = result.split('\n')
                    for line in lines:
                        if '|' in line and line.strip().startswith('|') and not 'count' in line and not '---' in line:
                            parts = line.split('|')
                            if len(parts) >= 2:
                                count_str = parts[1].strip()
                                try:
                                    count = int(count_str)
                                    break
                                except ValueError:
                                    continue
                    else:
                        count = 0
                else:
                    count = 0

                table_status[table] = count
                print(f"{table}: {count:,} records")

            except Exception as e:
                table_status[table] = f"Error: {e}"
                print(f"{table}: Error - {e}")

        views_to_check = ['team_stats', 'match_summary']
        for view in views_to_check:
            try:
                result = await database.execute_query(f"SELECT COUNT(*) as count FROM {view}", [], f"Count {view}")
                print(f"{view} view: Available")
            except Exception as e:
                print(f"{view} view: {e}")

        await database.disconnect()

        matches_count = table_status.get('matches', 0)
        deliveries_count = table_status.get('deliveries', 0)
        
        if isinstance(matches_count, int) and isinstance(deliveries_count, int) and matches_count > 0 and deliveries_count > 0:
            print("Database has sufficient data for testing")
            return True
        else:
            print("Database has insufficient data - run loader.py to import IPL data")
            return False

    except Exception as e:
        print(f"Database schema test failed: {e}")
        return False

async def test_query_processor():
    print("\nTesting Query Processor")
    print("=" * 30)
    
    try:
        from main import query_processor

        test_cases = [
            ("show me recent matches", "recent_matches"),
            ("top run scorers", "batting_stats"),
            ("team statistics", "team_performance"),
            ("matches between CSK and MI", "head_to_head"),
            ("player performance of Virat Kohli", "batting_stats"),
            ("venue statistics for Wankhede", "venue_stats"),
            ("season 2022 statistics", "season_summary")
        ]

        for query, expected_type in test_cases:
            try:
                query_type, params = query_processor.identify_query_type(query)
                sql, sql_params = query_processor.generate_sql(query_type, params)
                
                if query_type == expected_type:
                    print(f"'{query}' -> {query_type}")
                else:
                    print(f"'{query}' -> {query_type} (expected {expected_type})")
            except Exception as e:
                print(f"'{query}' -> Error: {e}")

        print("Query processor test completed")
        return True

    except ImportError as e:
        print(f"Import error: {e}")
        return False
    except Exception as e:
        print(f"Query processor test failed: {e}")
        return False

async def test_queries():
    print("\nTesting Query Execution")
    print("=" * 30)
    
    try:
        from main import query_processor, database

        await database.connect()
        print("Database connected")

        test_queries = [
            "show me recent matches",
            "top run scorers",
            "team statistics",
            "matches between CSK and MI",
            "player performance of Virat Kohli",
            "venue statistics for Wankhede",
            "season 2022 statistics"
        ]

        successful_queries = 0
        for i, query in enumerate(test_queries, 1):
            print(f"\n[{i}] Testing: '{query}'")
            try:
                query_type, params = query_processor.identify_query_type(query)
                sql, sql_params = query_processor.generate_sql(query_type, params)
                print(f"Query type: {query_type}")
                print(f"Parameters: {params}")

                result = await database.execute_query(sql, sql_params, f"Test Query: {query}")
                if result and len(result) > 50:
                    print(f"Results: Query executed successfully ({len(result)} chars)")
                    sample = result[:100].replace('\n', ' ')
                    print(f"Sample: {sample}...")
                    print("Query successful")
                    successful_queries += 1
                else:
                    print("Query returned minimal data")

            except Exception as e:
                print(f"Query failed: {e}")
                print(f"Error details: {str(e)[:100]}...")

        await database.disconnect()
        print(f"\nQuery Execution Summary: {successful_queries}/{len(test_queries)} successful")
        return successful_queries >= len(test_queries) // 2

    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure main.py is in the current directory")
        return False
    except Exception as e:
        print(f"Test error: {e}")
        print(f"Error details: {traceback.format_exc()}")
        return False

async def test_mcp_tool():
    print("\nTesting MCP Tool Function")
    print("=" * 30)
    
    try:
        from main import query_ipl_cricket_data

        test_queries = [
            "show me recent matches",
            "who are the top run scorers?",
            "team statistics for Mumbai"
        ]

        for query in test_queries:
            try:
                print(f"\nTesting tool with: '{query}'")
                result = await query_ipl_cricket_data(query)
                if result and len(result) > 0:
                    print(f"Tool returned {len(result)} characters of response")
                    preview = result[:200] + "..." if len(result) > 200 else result
                    print(f"Preview: {preview}")
                else:
                    print("Tool returned empty response")
            except Exception as e:
                print(f"Tool failed: {e}")

        return True

    except ImportError as e:
        print(f"Import error: {e}")
        return False
    except Exception as e:
        print(f"MCP tool test failed: {e}")
        return False

async def test_sql_tool():
    print("\nTesting Direct SQL Tool")
    print("=" * 30)
    
    try:
        from main import execute_direct_sql_query

        test_queries = [
            "SELECT COUNT(*) as total_matches FROM matches",
            "SELECT COUNT(*) as total_players FROM players",
            "DESCRIBE matches"
        ]

        for query in test_queries:
            try:
                print(f"\nTesting SQL: '{query}'")
                result = await execute_direct_sql_query(query)
                if result and "ERROR" not in result:
                    print(f"SQL executed successfully")
                    preview = result[:150] + "..." if len(result) > 150 else result
                    print(f"Result preview: {preview}")
                else:
                    print(f"SQL execution had issues: {result[:100]}...")
            except Exception as e:
                print(f"SQL execution failed: {e}")

        return True

    except ImportError as e:
        print(f"Import error: {e}")
        return False
    except Exception as e:
        print(f"SQL tool test failed: {e}")
        return False

async def test_schema_tool():
    print("\nTesting Schema Info Tool")
    print("=" * 30)
    
    try:
        from main import get_database_schema_info

        print("Testing schema info retrieval...")
        result = await get_database_schema_info()
        
        if result and len(result) > 100:
            print(f"Schema info retrieved successfully ({len(result)} chars)")
            
            if "matches" in result and "players" in result and "deliveries" in result:
                print("Schema contains expected table information")
            else:
                print("Schema may be incomplete")
        else:
            print("Schema info retrieval returned minimal data")

        return True

    except ImportError as e:
        print(f"Import error: {e}")
        return False
    except Exception as e:
        print(f"Schema tool test failed: {e}")
        return False

def main():
    print("IPLMCP Server Test Suite")
    print("=" * 50)

    if not Path("main.py").exists():
        print("main.py not found")
        print("Please run this script from the project directory")
        return

    print("main.py found")

    if not check_environment():
        print("\nEnvironment configuration failed")
        print("Please fix your .env file and try again")
        return

    if not check_dependencies():
        print("\nDependencies check failed")
        print("Please install missing dependencies and try again")
        return

    async def run_all_tests():
        tests = [
            ("Database Connection", test_database_connection()),
            ("Database Schema", test_database_schema()),
            ("Query Processor", test_query_processor()),
            ("Query Execution", test_queries()),
            ("MCP Tool Function", test_mcp_tool()),
            ("Direct SQL Tool", test_sql_tool()),
            ("Schema Info Tool", test_schema_tool())
        ]

        results = []
        for test_name, test_coro in tests:
            print(f"\n{'='*50}")
            try:
                result = await test_coro
                results.append((test_name, result))
            except Exception as e:
                print(f"{test_name} failed with exception: {e}")
                results.append((test_name, False))

        return results

    test_results = asyncio.run(run_all_tests())

    print("\n" + "=" * 50)
    print("TEST RESULTS SUMMARY")
    print("=" * 50)
    
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "PASSED" if result else "FAILED"
        print(f"{test_name:.<30} {status}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("\nAll tests passed! Your IPLMCP server is ready!")
        print("\nNext steps:")
        print("Restart Claude Desktop application")
        print("Try queries like:")
        print("- 'Show me recent matches'")
        print("- 'Who are the top run scorers?'")
        print("- 'Team statistics for Mumbai Indians'")
        print("- 'Matches between CSK and MI'")
        print("- 'SELECT * FROM matches LIMIT 5'")

    elif passed >= total * 0.7:
        print(f"\nMost tests passed ({passed}/{total})! Your server should work.")
        print("\nMinor issues detected:")
        failed_tests = [name for name, result in test_results if not result]
        for test in failed_tests:
            print(f"- {test}")
        print("\nYou can still proceed with testing the server.")

    else:
        print(f"\n{total - passed} test(s) failed")
        print("Please review the error messages above and fix the issues")

        failed_tests = [name for name, result in test_results if not result]
        
        if "Database Connection" in failed_tests:
            print("\nDatabase Connection Issues:")
            print("- Ensure MySQL is running")
            print("- Check DB credentials in .env file")
            print("- Verify database exists")
            print("- Run: mysql -u root -p")

        if "Database Schema" in failed_tests:
            print("\nDatabase Schema Issues:")
            print("- Run loader.py to import IPL data")
            print("- Check if all required tables exist")
            print("- Verify JSON files are in the correct location")

        if "Query Execution" in failed_tests:
            print("\nQuery Execution Issues:")
            print("- Ensure database has sufficient data")
            print("- Check SQL query syntax in main.py")
            print("- Test with direct SQL queries first")

        if "MCP Tool Function" in failed_tests:
            print("\nMCP Tool Issues:")
            print("- Check FastMCP setup in main.py")
            print("- Verify tool functions are properly defined")
            print("- Test individual components first")

    print(f"\nTest Configuration:")
    print(f"Database: {os.getenv('DB_NAME', 'ipl_final')}")
    print(f"Host: {os.getenv('DB_HOST', 'localhost')}")
    print(f"User: {os.getenv('DB_USER', 'root')}")
    print(f"Test Environment: {'Ready' if passed >= total * 0.7 else 'Needs Attention'}")

if __name__ == "__main__":
    main()
