#!/bin/bash

# Start pipelines
echo "🚀 Starting CDC Monitor..."
python3 complete_cdc_monitor.py &
CDC_PID=$!

echo "🚀 Starting Activity Event Pipeline..."
python3 activity_event_pipeline.py &
PIPELINE_PID=$!

# Wait for startup
sleep 5

# Verify pipelines
echo "🔍 Verifying pipelines..."
if ps -p $CDC_PID > /dev/null 2>&1; then
    echo "  ✅ CDC Monitor: Running (PID: $CDC_PID)"
else
    echo "  ❌ CDC Monitor: FAILED to start"
fi

if ps -p $PIPELINE_PID > /dev/null 2>&1; then
    echo "  ✅ Activity Pipeline: Running (PID: $PIPELINE_PID)"
else
    echo "  ❌ Activity Pipeline: FAILED to start"
fi

echo ""
echo "📋 Process Summary:"
echo "  🌉 Kafka Port-Forward: PID $KAFKA_PID"
echo "  🌉 MySQL Port-Forward: PID $MYSQL_PID"
echo "  📡 CDC Monitor: PID $CDC_PID"
echo "  ⚙️  Activity Pipeline: PID $PIPELINE_PID"
echo ""
echo "🎯 Pipeline startup complete!"
echo "📊 Check status: ps aux | grep -E '(complete_cdc_monitor|activity_event_pipeline)' | grep python"
echo "🛑 Stop all: ./stop_pipeline.sh"
