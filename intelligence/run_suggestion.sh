#!/bin/bash

# Run suggestion analysis every 5 minutes
echo "🚀 Starting Intelligence Suggestion Service (every 5 minutes)"
echo "Press Ctrl+C to stop"
echo ""

while true; do
    echo "$(date): 🤖 Running intelligence suggestion..."
    python3 suggestion.py
    
    if [ $? -eq 0 ]; then
        echo "$(date): ✅ Completed successfully"
    else
        echo "$(date): ❌ Failed with error code $?"
    fi
    
    echo "$(date): ⏳ Waiting 5 minutes..."
    echo ""
    sleep 300
done
