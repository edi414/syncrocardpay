# Card Payment Reconciliation System

Automated system for processing and reconciling electronic statements from credit/debit card transactions.

## Business Problem

This system solves the challenge of manually processing and reconciling card payment transaction files from multiple sources (FTPS servers and Google Drive), ensuring data integrity and providing a centralized database for financial analysis and reporting.

## Architecture

```
syncrocardpay/
├── main.py                    # Main orchestration script
├── scripts/
│   ├── leitor_extratos.py     # Transaction processing logic
│   ├── reading_files.py       # File parsing and validation
│   └── transform_files.py     # Data transformation and validation
├── utils/
│   ├── connection_db.py       # Database operations
│   └── logger.py             # Logging configuration
├── outputs/log/              # Execution logs
└── setup_cronjob.sh         # Cronjob setup script
```

## Technologies

- **Python 3.x** - Core programming language
- **PostgreSQL** - Primary database (via psycopg2)
- **pandas** - Data manipulation and processing
- **FTPS** - Secure file transfer protocol for downloading statements
- **Google Drive API** - Cloud storage integration
- **cron** - Task scheduling (Unix/macOS)
- **python-dotenv** - Environment variable management
- **logging** - Comprehensive logging system

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd syncrocardpay
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**
   ```bash
   cp env.example .env
   ```
   
   Edit `.env` with your credentials:
   ```env
   HOST=your_ftps_host
   FTPS_USER=your_ftps_username
   FTPS_PASSWORD=your_ftps_password
   FTPS_PORT=21
   
   DB_HOST=your_postgres_host
   DB_USER=your_postgres_user
   DB_PASSWORD=your_postgres_password
   DB_NAME=your_database
   DB_PORT=5432
   
   GOOGLE_DRIVE_DIRECTORY=/path/to/google/drive/folder
   ```

## Process Flow

1. **File Discovery**: Connects to FTPS server and Google Drive to identify new transaction files
2. **File Synchronization**: Copies files from Google Drive to local processing directory
3. **Data Processing**: Parses transaction files, validates data structure and content
4. **Database Insertion**: Stores processed transactions in PostgreSQL with dimensional modeling
5. **File Management**: Tracks processed files to avoid reprocessing
6. **Logging**: Comprehensive logging of all operations and errors

## Data Model

The system implements a dimensional data model with:
- **Fact Table**: Transaction details (amounts, dates, card info, etc.)
- **Dimension Tables**: Time, Store, Product, and Payment method dimensions
- **Control Tables**: File processing status and metadata

## Usage

### Manual Execution
```bash
python main.py
```

### Automated Execution (macOS)
```bash
chmod +x setup_cronjob.sh
./setup_cronjob.sh
```

## Logging

Logs are saved in `outputs/log/` with timestamp format:
```
log_DDMMYY_HH_MM_SS.txt
```

## Configuration

### Cronjob Setup for macOS
The `setup_cronjob.sh` script configures automatic execution. To run daily at 09:30 BR time:

```bash
# Edit the script to change execution time
# Current: 0 2 * * * (02:00 UTC)
# For 09:30 BR time (12:30 UTC): 30 12 * * *
```

## Important Notes

- Never commit real credentials to the repository
- Always use `.env` file for sensitive configurations
- Monitor logs regularly for processing status
- Ensure PostgreSQL database is accessible
- Google Drive folder must be properly configured

## Development

### Adding New Validations
Edit `scripts/transform_files.py` to add new data validation rules.

### Modifying Data Structure
Edit `scripts/reading_files.py` to change file parsing logic.

## License

Internal use only.
