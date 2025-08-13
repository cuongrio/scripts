#!/bin/bash

# Start pipelines
echo "🚀 Starting CDC Monitor..."
python3 complete_cdc_monitor.py &
CDC_PID=$!

# Check if CDC process started successfully
if [ -z "$CDC_PID" ]; then
    echo "  ❌ CDC Monitor: Failed to get PID"
    CDC_PID=""
else
    echo "  📡 CDC Monitor started with PID: $CDC_PID"
fi

echo "🚀 Starting Activity Event Pipeline..."
python3 activity_event_pipeline.py &
PIPELINE_PID=$!

# Check if Pipeline process started successfully  
if [ -z "$PIPELINE_PID" ]; then
    echo "  ❌ Activity Pipeline: Failed to get PID"
    PIPELINE_PID=""
else
    echo "  ⚙️ Activity Pipeline started with PID: $PIPELINE_PID"
fi

# Wait for startup
sleep 5

# Verify pipelines
echo "🔍 Verifying pipelines..."

# Check CDC Monitor
if [ -n "$CDC_PID" ] && ps -p $CDC_PID > /dev/null 2>&1; then
    echo "  ✅ CDC Monitor: Running (PID: $CDC_PID)"
elif [ -z "$CDC_PID" ]; then
    echo "  ❌ CDC Monitor: PID is null/empty"
else
    echo "  ❌ CDC Monitor: Process not found (PID: $CDC_PID)"
fi

# Check Activity Pipeline
if [ -n "$PIPELINE_PID" ] && ps -p $PIPELINE_PID > /dev/null 2>&1; then
    echo "  ✅ Activity Pipeline: Running (PID: $PIPELINE_PID)"
elif [ -z "$PIPELINE_PID" ]; then
    echo "  ❌ Activity Pipeline: PID is null/empty"
else
    echo "  ❌ Activity Pipeline: Process not found (PID: $PIPELINE_PID)"
fi

echo ""
echo "📋 Process Summary:"
if [ -n "$CDC_PID" ]; then
    echo "  📡 CDC Monitor: PID $CDC_PID"
else
    echo "  📡 CDC Monitor: NOT RUNNING"
fi

if [ -n "$PIPELINE_PID" ]; then
    echo "  ⚙️  Activity Pipeline: PID $PIPELINE_PID"
else
    echo "  ⚙️  Activity Pipeline: NOT RUNNING"
fi
echo ""
echo "🎯 Pipeline startup complete!"
echo "📊 Check status: ps aux | grep -E '(complete_cdc_monitor|activity_event_pipeline)' | grep python"
echo "🛑 Stop all: ./stop_pipeline.sh"
