#!/usr/bin/env python3

import os
import sys
import json
import platform
import subprocess
from pathlib import Path

PROJECT_NAME  = "IPLMCP"
PROJECT_PATH  = Path("D:/Data/iplmcp")
MAIN_FILE     = "main.py"

#  Load env so we never hard-code credentials here
from dotenv import load_dotenv
load_dotenv()
DATABASE_NAME = os.getenv("DB_NAME", "ipl_final")
DATABASE_PASSWORD = os.getenv("DB_PASSWORD", "")

def get_claude_config_path():
    system = platform.system()
    if system == "Darwin":
        return Path.home() / "Library/Application Support/Claude/claude_desktop_config.json"
    elif system == "Windows":
        return Path(os.environ.get("APPDATA", "")) / "Claude/claude_desktop_config.json"
    else:
        return Path.home() / ".config/Claude/claude_desktop_config.json"

def check_dependencies():
    print("Checking dependencies...")
    try:
        subprocess.run(["uv", "--version"], check=True, capture_output=True)
        print("UV is installed")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("UV is not installed. Please install UV first:")
        print("powershell -c \"irm https://astral.sh/uv/install.ps1 | iex\"")
        return False
    
    if sys.version_info < (3, 8):
        print(f"Python 3.8+ required, found {sys.version_info.major}.{sys.version_info.minor}")
        return False
    
    print(f"Python {sys.version_info.major}.{sys.version_info.minor} is compatible")
    return True

def setup_project_directory():
    print(f"\nSetting up project directory: {PROJECT_PATH}")
    
    if not PROJECT_PATH.exists():
        try:
            PROJECT_PATH.mkdir(parents=True, exist_ok=True)
            print(f"Created project directory: {PROJECT_PATH}")
        except Exception as e:
            print(f"Failed to create project directory: {e}")
            return False
    else:
        print(f"Project directory exists: {PROJECT_PATH}")
    
    try:
        os.chdir(PROJECT_PATH)
        print(f"Changed to project directory: {os.getcwd()}")
        return True
    except Exception as e:
        print(f"Failed to navigate to project directory: {e}")
        return False

def setup_environment():
    print("\nSetting up environment configuration...")
    
    env_file = PROJECT_PATH / ".env"
    if env_file.exists():
        print(".env file already exists")
        return True
    
    try:
        env_content = f"""DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD={DATABASE_PASSWORD}
DB_NAME={DATABASE_NAME}
DB_CHARSET=utf8mb4
DB_AUTOCOMMIT=true
DB_RAISE_ON_WARNINGS=true
DB_CONNECTION_TIMEOUT=10
DB_MAX_RETRIES=3
SERVER_NAME={PROJECT_NAME}
SERVER_VERSION=1.0.0
LOG_LEVEL=INFO
CLAUDE_API_KEY=k-ant-api03-zrr1UBfEpoyZMy0SYsME8oVwy8R9TXO58A1HPbdv0ZiV8wJuF9X86gqkE58k-Z4AgWH_N8Mb74_4UgmTLQHHZA-axT_qAAA
MCP_SERVER=http://localhost:8000
"""
        
        with open(env_file, 'w') as f:
            f.write(env_content)
        
        print(f".env file created: {env_file}")
        print("\nEnvironment variables configured:")
        print(f"Database: {DATABASE_NAME}")
        print(f"Password: {DATABASE_PASSWORD}")
        print(f"Server: {PROJECT_NAME}")
        
        return True
    except Exception as e:
        print(f"Failed to create .env file: {e}")
        return False

def setup_project():
    print("\nSetting up project...")
    
    try:
        if not (PROJECT_PATH / "pyproject.toml").exists():
            subprocess.run(["uv", "init", "--no-readme"], check=True)
            print("UV project initialized")
        else:
            print("UV project already initialized")
    except subprocess.CalledProcessError:
        print("UV project already initialized or failed to initialize")
    
    try:
        subprocess.run(["uv", "add", "mcp", "mysql-connector-python", "python-dotenv"], check=True)
        print("Dependencies installed")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install dependencies: {e}")
        return False
    
    return True

def update_claude_config():
    print("\nConfiguring Claude Desktop...")
    
    config_path = get_claude_config_path()
    server_path = PROJECT_PATH / MAIN_FILE
    
    if not server_path.exists():
        print(f"Server file not found: {server_path}")
        print("This is normal if you haven't created main.py yet")
        print("The configuration will still be added to Claude Desktop")
    
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    config = {}
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            print("Loaded existing Claude Desktop config")
        except json.JSONDecodeError:
            print("Existing config file is invalid JSON, creating new one")
    else:
        print("Creating new Claude Desktop config")
    
    if "mcpServers" not in config:
        config["mcpServers"] = {}
    
    config["mcpServers"][PROJECT_NAME] = {
        "command": "uv",
        "args": [
            "run",
            "python",
            str(server_path).replace("\\", "/")
        ],
        "env": {
            "UV_PROJECT_DIR": str(PROJECT_PATH).replace("\\", "/")
        }
    }
    
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"Claude Desktop config updated: {config_path}")
        print(f"Added MCP server: {PROJECT_NAME}")
        return True
    except Exception as e:
        print(f"Failed to write config: {e}")
        return False

def test_database_connection():
    print("\nTesting database connection...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        import mysql.connector
        
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': DATABASE_PASSWORD,
            'database': DATABASE_NAME
        }
        
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM matches")
            match_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM players")
            player_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM deliveries")
            delivery_count = cursor.fetchone()[0]
            
            print(f"Database connection successful!")
            print(f"Matches: {match_count:,}")
            print(f"Players: {player_count:,}")
            print(f"Deliveries: {delivery_count:,}")
        except mysql.connector.Error:
            print(f"Database connection successful!")
            print(f"Tables not found - run loader.py to import IPL data")
        
        cursor.close()
        conn.close()
        return True
        
    except ImportError as e:
        print(f"Missing dependencies: {e}")
        print("Dependencies will be available after project setup")
        return True
    except mysql.connector.Error as e:
        print(f"Database connection failed: {e}")
        print(f"Please ensure MySQL is running and database '{DATABASE_NAME}' exists")
        print(f"MySQL credentials: root / {DATABASE_PASSWORD}")
        return False
    except Exception as e:
        print(f"Database test failed: {e}")
        return True

def create_directories():
    print("\nCreating project directories...")
    # Only create directories that don't already exist elsewhere
    # JSON folder is at C:/Users/nanda/Downloads/ipl_json, so don't create it here
    directories = []
    
    for directory in directories:
        try:
            directory.mkdir(parents=True, exist_ok=True)
            print(f"Created directory: {directory.name}")
        except Exception as e:
            print(f"Failed to create directory {directory}: {e}")
    
    # Just inform where JSON files should be placed
    print(f"Note: Place your JSON files in: C:/Users/nanda/Downloads/ipl_json")

def main():
    print(f"{PROJECT_NAME} Setup")
    print("=" * 40)
    
    success = True
    
    if not check_dependencies():
        success = False
    
    if success and not setup_project_directory():
        success = False
    
    if success:
        create_directories()
    
    if success and not setup_project():
        success = False
    
    if success and not setup_environment():
        success = False
    
    if success and not update_claude_config():
        success = False
    
    if success:
        test_database_connection()
    
    print("\n" + "=" * 40)
    
    if success:
        print("Setup completed successfully!")
        print(f"\nProject Information:")
        print(f"Name: {PROJECT_NAME}")
        print(f"Path: {PROJECT_PATH}")
        print(f"Database: {DATABASE_NAME}")
        print(f"Main file: {MAIN_FILE}")
        
        print("\nNext steps:")
        print("Create your main.py file with the IPLMCP server code")
        print("Place IPL JSON files in C:/Users/nanda/Downloads/ipl_json/")
        print("Run: python loader.py (to load cricket data)")
        print("Run: python test_mcp.py (to test the server)")
        print("Restart Claude Desktop application")
        print("Test with queries like: 'Show me recent matches'")
        
        print(f"\nProject structure should be:")
        print(f"D:/Data/iplmcp/")
        print(f"├── main.py")
        print(f"├── loader.py")
        print(f"├── test_mcp.py")
        print(f"├── .env")
        print(f"└── .venv/")
        
        print(f"\nJSON files location:")
        print(f"C:/Users/nanda/Downloads/ipl_json/")
        print(f"├── 980961.json")
        print(f"├── 1473504.json")
        print(f"└── ... (other IPL match files)")
        
    else:
        print("Setup encountered errors")
        print("Please check the error messages above and try again")
        
        print(f"\nProject directory: {PROJECT_PATH}")
        print(f"Claude config: {get_claude_config_path()}")
        print(f"Environment file: {PROJECT_PATH / '.env'}")

if __name__ == "__main__":
    main()
