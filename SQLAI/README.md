# Multi-DB SQL AI Agent 🤖

A powerful AI-powered FastAPI application that translates natural language into SQL, executes queries against multiple database dialects (PostgreSQL, MySQL, Oracle, SQL Server), generates intelligent visualizations, and features self-healing capabilities for failed queries.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)

---

## 🌟 Features

- **🧠 Natural Language to SQL**: Convert plain English questions into optimized SQL queries using Google's Gemini AI
- **🔄 Self-Healing Queries**: Automatic retry mechanism with AI-powered error correction
- **📊 Smart Visualizations**: AI-generated charts and graphs using Matplotlib/Seaborn
- **🗄️ Multi-Database Support**: PostgreSQL, MySQL, Oracle, SQL Server with automatic dialect detection
- **🔒 Safe Mode**: Read-only query enforcement for production environments
- **⚡ Schema Caching**: Intelligent caching system using Neon Postgres for faster responses
- **📄 Server-Side Pagination**: Efficient data browsing with dialect-aware pagination
- **🎨 Modern Frontend**: Beautiful, responsive UI with real-time query execution
- **📈 Auto-Dashboard**: Generate comprehensive dashboards with 5 AI-curated insights
- **🔧 SQL Optimizer**: Analyze and optimize SQL queries for better performance

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Database connection (PostgreSQL, MySQL, Oracle, or SQL Server)
- Google Gemini API key ([Get one here](https://ai.google.dev/))

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd SQLai
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment variables**

Create a `.env` file in the root directory:
```env
GEMINI_API_KEY=your_gemini_api_key_here
CACHE_DB_URL=postgresql://user:password@host:port/dbname
```

5. **Run the application**
```bash
python app2.py
```

The application will be available at `http://localhost:8000`

---

## 🏗️ Architecture

### Project Structure

```
SQLai/
├── app2.py                 # Main FastAPI application
├── models.py               # Pydantic models for request/response
├── database_manager.py     # Database connection & schema management
├── cache_manager.py        # Schema caching with Neon Postgres
├── ai_service.py           # Google Gemini AI integration
├── viz_service.py          # Visualization generation
├── utils.py                # Helper functions
├── config.py               # Configuration management
├── requirements.txt        # Python dependencies
├── frontend/
│   ├── index.html          # Main UI
│   ├── app.js              # Frontend logic
│   └── styles.css          # Styling
└── .env                    # Environment variables
```

### Technology Stack

**Backend:**
- FastAPI - Modern, fast web framework
- SQLAlchemy - Database ORM and query builder
- Pandas - Data manipulation and analysis
- Google Gemini AI - Natural language processing
- Matplotlib/Seaborn - Data visualization

**Frontend:**
- Vanilla JavaScript - Interactive UI
- CSS3 - Modern styling with animations
- HTML5 - Semantic markup

**Databases:**
- PostgreSQL
- MySQL
- Oracle
- SQL Server

---

## 📡 API Endpoints

### Base URL
`http://localhost:8000`

### 1. Get All Schemas
**`POST /schemas`**

Fetches the database schema (tables and columns) for a given connection string.

**Request:**
```json
{
  "db_url": "postgresql://user:password@host:port/dbname"
}
```

**Response:**
```json
{
  "tables": {
    "users": [
      {"name": "id", "type": "INTEGER"},
      {"name": "username", "type": "VARCHAR(50)"}
    ]
  }
}
```

---

### 2. Get Table Details
**`POST /schemas/{table_name}`**

Provides metadata, row count, and preview (first 10 and last 10 rows) for a specific table.

**Request:**
```json
{
  "db_url": "postgresql://user:password@host:port/dbname"
}
```

**Response:**
```json
{
  "table_name": "users",
  "row_count": 1500,
  "columns": ["id", "username", "email"],
  "first_10": [...],
  "last_10": [...]
}
```

---

### 3. Get Paginated Table Data
**`POST /schemas/{table_name}/data`** ⭐ NEW

Server-side pagination with dialect-aware LIMIT/OFFSET handling.

**Request:**
```json
{
  "db_url": "postgresql://user:password@host:port/dbname",
  "page": 1,
  "limit": 100
}
```

**Response:**
```json
{
  "data": [...],
  "total_rows": 1500,
  "page": 1,
  "total_pages": 15,
  "error": null
}
```

---

### 4. Generate & Execute Analysis
**`POST /generate`**

Translates natural language into SQL, executes it, and returns data with AI-generated visualizations. Features **self-healing** with automatic retry on failure.

**Request:**
```json
{
  "db_url": "postgresql://user:password@host:port/dbname",
  "query": "Show me the top 5 users by spend in the last month",
  "safe_mode": true
}
```

**Response:**
```json
{
  "sql_query": "SELECT user_id, SUM(amount) as total FROM orders...",
  "message": "Data retrieved successfully.",
  "data_preview": [...],
  "graphs_base64": ["base64_encoded_image..."],
  "csv_base64": "base64_encoded_csv...",
  "error": null
}
```

---

### 5. Generate Dashboard
**`POST /gen-dashboard`**

Automatically discovers insights and generates 5 professional charts.

**Request:**
```json
{
  "db_url": "postgresql://user:password@host:port/dbname"
}
```

**Response:**
```json
{
  "charts": [
    {
      "title": "Revenue Growth Over Time",
      "description": "Month-over-month revenue trends",
      "graph_base64": "..."
    }
  ],
  "error": null
}
```

---

### 6. Optimize SQL
**`POST /optimize`**

Analyzes SQL queries for performance bottlenecks and logical errors.

**Request:**
```json
{
  "db_url": "postgresql://user:password@host:port/dbname",
  "query": "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
}
```

**Response:**
```json
{
  "original_query": "SELECT * FROM users...",
  "optimized_query": "SELECT u.id, u.username FROM users u...",
  "explanation": "Replaced subquery with JOIN for better performance...",
  "difference_score": 45
}
```

---

## 💡 Usage Examples

### Example 1: Natural Language Query
```bash
curl -X POST http://localhost:8000/generate \
  -H "Content-Type: application/json" \
  -d '{
    "db_url": "postgresql://user:pass@localhost:5432/mydb",
    "query": "What are the top 10 products by revenue this year?",
    "safe_mode": true
  }'
```

### Example 2: Browse Table with Pagination
```bash
curl -X POST http://localhost:8000/schemas/orders/data \
  -H "Content-Type: application/json" \
  -d '{
    "db_url": "postgresql://user:pass@localhost:5432/mydb",
    "page": 2,
    "limit": 50
  }'
```

### Example 3: Generate Dashboard
```bash
curl -X POST http://localhost:8000/gen-dashboard \
  -H "Content-Type: application/json" \
  -d '{
    "db_url": "postgresql://user:pass@localhost:5432/mydb"
  }'
```

---

## 🎨 Frontend Features

The modern web interface includes:

- **Schema Explorer**: Browse database tables and columns with pagination
- **AI Query Interface**: Natural language to SQL with real-time execution
- **Visualization Gallery**: Auto-generated charts and graphs
- **SQL Optimizer**: Analyze and improve query performance
- **Dashboard Generator**: One-click comprehensive insights
- **Dark Mode UI**: Modern, responsive design with smooth animations
- **Export Options**: Download results as CSV

Access the frontend at `http://localhost:8000`

---

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GEMINI_API_KEY` | Google Gemini API key | Required |
| `CACHE_DB_URL` | PostgreSQL cache database URL | Neon Postgres |
| `MODEL_NAME` | Gemini model to use | `gemini-2.5-flash` |

### Safe Mode

When `safe_mode: true`, the system:
- Only allows SELECT queries
- Blocks INSERT, UPDATE, DELETE, DROP, ALTER
- Validates queries before execution

---

## 🛡️ Security Features

- **SQL Injection Prevention**: Parameterized queries and table name validation
- **Safe Mode Enforcement**: Read-only operations in production
- **Connection String Validation**: Automatic dialect detection and validation
- **Error Sanitization**: Sensitive information removed from error messages

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

## 📝 License

This project is licensed under the MIT License.

---

## 🙏 Acknowledgments

- Google Gemini AI for natural language processing
- FastAPI for the excellent web framework
- Neon Postgres for serverless database caching
- The open-source community

---

## 📧 Support

For issues, questions, or suggestions, please open an issue on GitHub.

---

**Built with ❤️ using FastAPI and Google Gemini AI**
