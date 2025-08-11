IPL Cricket Database MCP Server Setup
Quick Setup Guide
Prerequisites
Python 3.11+

MySQL 8.0+

Claude Desktop

1. Install Dependencies
bash
pip install mysql-connector-python python-dotenv fastmcp mcp
2. Configure Environment
Copy .env file and update:

DB_PASSWORD=your_mysql_password

CLAUDE_API_KEY=your_claude_key

JSON_FOLDER=path_to_ipl_json_files

3. Setup Database
bash
python loader.py
This creates the database and imports IPL match data from JSON files.

4. Test Setup
bash
python test_mcp.py
Verify all components work correctly.

5. Configure Claude Desktop
Update claude_desktop_config.json:

json
{
  "mcpServers": {
    "IPLMCP": {
      "command": "python",
      "args": ["path/to/main.py"]
    }
  }
}
6. Start Server
Restart Claude Desktop

Ask queries like:

"Show recent matches"

"Top run scorers in IPL 2023"

"CSK vs MI head to head"

Files Overview
main.py - MCP server with cricket query tools

loader.py - Imports JSON data to MySQL

test_mcp.py - Tests all functionality

.env - Database and API configuration

Troubleshooting
Ensure MySQL is running

Check .env file has correct credentials

Run test_mcp.py to identify issues

IPL JSON files must be in specified folder

That's it! Your IPL cricket database is ready for natural language queries.
