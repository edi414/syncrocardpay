# Card Payment Reconciliation System

Automated system for processing and reconciling electronic statements from credit/debit card transactions using AWS Lambda and S3.

## Business Problem

This system solves the challenge of manually processing and reconciling card payment transaction files from multiple sources (FTPS servers and S3), ensuring data integrity and providing a centralized database for financial analysis and reporting.

## Architecture

```
syncrocardpay/
├── main.py                    # Main orchestration script (Lambda handler)
├── scripts/
│   ├── leitor_extratos.py     # Transaction processing logic
│   ├── reading_files.py       # File parsing and validation
│   ├── transform_files.py     # Data transformation and validation
│   ├── setup_parameters.sh    # AWS Parameter Store setup
│   ├── deploy.sh             # Serverless deployment script
│   └── local_run_s3.sh       # Local execution with S3
├── utils/
│   ├── connection_db.py       # Database operations
│   ├── logger.py             # Logging configuration
│   └── s3_utils.py           # S3 operations
├── queries/
│   └── create_schema.sql     # Database schema
├── serverless.yml            # AWS Lambda configuration
├── requirements.txt          # Python dependencies
└── env.example              # Environment variables template
```

## Technologies

- **Python 3.11** - Core programming language
- **PostgreSQL** - Primary database (via psycopg2)
- **pandas** - Data manipulation and processing
- **FTPS** - Secure file transfer protocol for downloading statements
- **AWS S3** - Cloud storage for processed files
- **AWS Lambda** - Serverless execution environment
- **AWS EventBridge** - Scheduled execution (daily at 09:00 BR time)
- **AWS Parameter Store** - Secure credential storage
- **Serverless Framework** - Infrastructure as Code
- **boto3** - AWS SDK for Python

## Installation

### Prerequisites

1. **AWS CLI configured** with appropriate permissions
2. **Node.js** for Serverless Framework
3. **Python 3.11** for local development

### Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd syncrocardpay
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   npm install
   ```

3. **Configure AWS credentials:**
   ```bash
   aws configure
   ```

4. **Set up Parameter Store:**
   ```bash
   chmod +x scripts/setup_parameters.sh
   ./scripts/setup_parameters.sh
   ```

5. **Deploy to AWS:**
   ```bash
   chmod +x scripts/deploy.sh
   ./scripts/deploy.sh
   ```

## Environment Variables

The system uses AWS Parameter Store for secure credential management:

### FTPS Configuration
- `/syncrocardpay/ftps/host` - FTPS server hostname
- `/syncrocardpay/ftps/user` - FTPS username
- `/syncrocardpay/ftps/password` - FTPS password (SecureString)
- `/syncrocardpay/ftps/port` - FTPS port (default: 21)

### Database Configuration
- `/syncrocardpay/db/host` - PostgreSQL hostname
- `/syncrocardpay/db/user` - Database username
- `/syncrocardpay/db/password` - Database password (SecureString)
- `/syncrocardpay/db/name` - Database name
- `/syncrocardpay/db/port` - Database port (default: 5432)

### S3 Configuration
- `S3_BUCKET` - S3 bucket name (hardcoded: `syncrocardpay-reports-244641534401`)
- `S3_PREFIX` - S3 prefix for processed files (default: `processed_files`)

## Process Flow

1. **File Discovery**: Connects to FTPS server and S3 to identify new transaction files
2. **File Download**: Downloads files from S3 (if available) or FTPS for processing
3. **Data Processing**: Parses transaction files, validates data structure and content
4. **Database Insertion**: Stores processed transactions in PostgreSQL with dimensional modeling
5. **File Upload**: Uploads processed files to S3 for archival
6. **File Management**: Tracks processed files to avoid reprocessing
7. **Logging**: Comprehensive logging of all operations and errors

## Data Model

The system implements a dimensional data model with:
- **Fact Table**: Transaction details (amounts, dates, card info, etc.)
- **Dimension Tables**: Time, Store, Product, and Payment method dimensions
- **Control Tables**: File processing status and metadata

## Usage

### AWS Lambda (Production)
The system runs automatically via AWS EventBridge:
- **Schedule**: Daily at 09:00 BR time (12:00 UTC)
- **Function**: `syncrocardpay-dev-reconcile`
- **Trigger**: EventBridge cron expression: `cron(0 12 * * ? *)`

### Manual Lambda Execution
```bash
aws lambda invoke --function-name syncrocardpay-dev-reconcile --payload '{}' response.json
```

### Local Development
```bash
# Set environment variables
export AWS_REGION=us-east-1
export S3_BUCKET=syncrocardpay-reports-244641534401
export S3_PREFIX=processed_files

# Run locally
chmod +x scripts/local_run_s3.sh
./scripts/local_run_s3.sh
```

### Manual Execution (Legacy)
```bash
python main.py
```

## Deployment

### Initial Deployment
```bash
./scripts/deploy.sh
```

### Redeploy
```bash
serverless deploy
```

### Remove Deployment
```bash
serverless remove
```

## Monitoring

### CloudWatch Logs
- **Log Group**: `/aws/lambda/syncrocardpay-dev-reconcile`
- **Log Retention**: 14 days (configurable)

### Lambda Metrics
- **Duration**: ~18 seconds average
- **Memory**: 161 MB peak usage (1024 MB allocated)
- **Success Rate**: Monitor via CloudWatch

## Configuration

### EventBridge Schedule
The Lambda is triggered daily at 09:00 BR time via EventBridge:
```yaml
events:
  - schedule:
      rate: cron(0 12 * * ? *)
      description: "Executa reconciliação diária às 09:00 BR time (12:00 UTC)"
```

### S3 Bucket
- **Name**: `syncrocardpay-reports-244641534401`
- **Region**: `us-east-1`
- **Access**: Lambda has full read/write permissions

## Security

- **Credentials**: Stored in AWS Parameter Store (encrypted)
- **IAM Roles**: Least privilege access
- **S3**: Bucket policies restrict access to Lambda function
- **VPC**: Database access via security groups

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are in `requirements.txt`
2. **Permission Errors**: Check IAM roles and S3 bucket policies
3. **Database Connection**: Verify Parameter Store credentials
4. **File System Errors**: Lambda uses `/tmp` for temporary files

### Logs
```bash
# View recent logs
aws logs describe-log-streams --log-group-name "/aws/lambda/syncrocardpay-dev-reconcile" --order-by LastEventTime --descending --max-items 1

# Get log events
aws logs get-log-events --log-group-name "/aws/lambda/syncrocardpay-dev-reconcile" --log-stream-name "STREAM_NAME"
```

## Development

### Adding New Validations
Edit `scripts/transform_files.py` to add new data validation rules.

### Modifying Data Structure
Edit `scripts/reading_files.py` to change file parsing logic.

### Local Testing
Use `scripts/local_run_s3.sh` for local development with S3 integration.

## Cost Optimization

- **Lambda**: Pay per execution (~$0.0000166667 per GB-second)
- **S3**: Pay per storage and requests
- **EventBridge**: Free for basic scheduling
- **Parameter Store**: Free for standard parameters

## License

Internal use only.