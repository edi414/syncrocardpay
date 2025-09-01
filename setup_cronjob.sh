#!/bin/bash

# Script to configure cronjob for Card Payment Reconciliation System
# Executes daily at 09:30 BR time (12:30 UTC)

echo "🔧 Configuring Cronjob for Card Payment Reconciliation System..."

# Get current directory
CURRENT_DIR=$(pwd)
PYTHON_PATH=$(which python3)

# Check if Python was found
if [ -z "$PYTHON_PATH" ]; then
    echo "❌ Python3 not found. Install Python3 first."
    exit 1
fi

# Create cronjob command (09:30 BR time = 12:30 UTC)
# Using conda environment
CRON_COMMAND="30 12 * * * cd $CURRENT_DIR && /opt/miniconda3/bin/conda run -n acaso_python_311 python3 main.py >> $CURRENT_DIR/outputs/log/cron.log 2>&1"

# Check if cronjob already exists
if crontab -l 2>/dev/null | grep -q "main.py"; then
    echo "⚠️  Cronjob already exists. Removing previous version..."
    crontab -l 2>/dev/null | grep -v "main.py" | crontab -
fi

# Add new cronjob
(crontab -l 2>/dev/null; echo "$CRON_COMMAND") | crontab -

echo "✅ Cronjob configured successfully!"
echo ""
echo "📋 Cronjob Details:"
echo "   Command: $CRON_COMMAND"
echo "   Execution: Daily at 09:30 BR time (12:30 UTC)"
echo "   Log: $CURRENT_DIR/outputs/log/cron.log"
echo ""
echo "📝 To check the cronjob:"
echo "   crontab -l"
echo ""
echo "📝 To remove the cronjob:"
echo "   crontab -r"
echo ""
echo "📝 To test manually:"
echo "   python3 main.py"
