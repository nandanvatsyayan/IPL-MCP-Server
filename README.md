# IPL Cricket Database MCP Server

## 📌 Objective
Build an **MCP (Model Context Protocol) server** that answers questions about IPL cricket match data in **natural English**, integrated with **Claude Desktop**.

---

## 📂 Dataset
- Source: [CricSheet IPL Match Data](https://cricsheet.org/matches/)
- Format: JSON
- Use data from at least 5 matches (entire dataset optional)

---

## ⚙️ Requirements

### **1. Data Processing & Storage**
- Parse the JSON cricket match data
- Create SQL database schema (**MySQL / PostgreSQL / SQLite**)
- Store the data efficiently for querying

### **2. MCP Server Development**
- Build an MCP server that queries the cricket database
- Translate natural language to SQL queries
- Return readable results

### **3. Claude Desktop Integration**
- Configure MCP server to connect to Claude Desktop

---

## 🏗 Setup Instructions

### **1. Prerequisites**
- Python 3.11+
- MySQL 8.0+
- Claude Desktop installed

### **2. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **3. Configure Environment**
Create a `.env` file:
```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=ipl_final
DB_CHARSET=utf8mb4
DB_AUTOCOMMIT=true
DB_RAISE_ON_WARNINGS=false
DB_CONNECTION_TIMEOUT=10
DB_MAX_RETRIES=3

SERVER_NAME=IPLMCP
SERVER_VERSION=1.0.0
LOG_LEVEL=INFO
MCP_SERVER=http://localhost:8000

CLAUDE_API_KEY=your_claude_api_key
```

### **4. Load IPL Data**
Download at least 5 IPL match JSON files from [CricSheet](https://cricsheet.org/matches/) and place them in a folder.

Run the loader:
```bash
python loader.py
```

### **5. Start the MCP Server**
```bash
python main.py
```

### **6. Connect to Claude Desktop**
Add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "IPLMCP": {
      "command": "python",
      "args": [
        "path/to/main.py"
      ]
    }
  }
}
```

---

## 🔍 Sample Queries

### **Basic Match Information**
- Show me all matches in the dataset
- Which team won the most matches?
- What was the highest total score?
- Show matches played in Mumbai

### **Player Performance**
- Who scored the most runs across all matches?
- Which bowler took the most wickets?
- Show me Virat Kohli's batting stats
- Who has the best bowling figures in a single match?

### **Advanced Analytics**
- What's the average first innings score?
- Which venue has the highest scoring matches?
- Show me all centuries scored
- What's the most successful chase target?
- Which team has the best powerplay performance?

### **Match-Specific Queries**
- Show me the scorecard for match between CSK and MI
- How many sixes were hit in the final?
- What was the winning margin in the closest match?
- Show partnerships over 100 runs

---

## 📜 Deliverables
1. **Setup Instructions** – README with installation steps, DB setup, Claude connection
2. **Test Queries** – Example queries to validate server capabilities

---

## 🧪 Testing
Run the included test suite:
```bash
python test_mcp.py
```

