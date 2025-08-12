#!/bin/bash

# 🚀 Kafka CDC Pipeline Startup Script

echo "🔧 Setting up Kafka CDC Pipeline..."

# Clear proxy settings
echo "🧹 Clearing proxy settings..."
unset HTTPS_PROXY
unset HTTP_PROXY

# Kill existing port-forwards
echo "🔄 Cleaning up existing port-forwards..."
pkill -f "kubectl port-forward.*kafka" 2>/dev/null || true
pkill -f "kubectl port-forward.*mysql" 2>/dev/null || true

# Kill existing pipelines
echo "🛑 Stopping existing pipelines..."
pkill -f "complete_cdc_monitor" 2>/dev/null || true
pkill -f "activity_event_pipeline" 2>/dev/null || true

# Wait for cleanup
sleep 3

# Setup port-forwards
echo "🌉 Setting up port-forwards..."

# Kafka (dev namespace)
echo "  📡 Starting Kafka port-forward..."
kubectl port-forward services/kafka 9092:9092 -n dev &
KAFKA_PID=$!

# MySQL (qc namespace)  
echo "  🗄️  Starting MySQL port-forward..."
kubectl port-forward services/mysql 3306:3306 -n qc &
MYSQL_PID=$!

# Wait for connections to be ready
echo "⏳ Waiting for connections to be ready..."
sleep 10

# Verify connections
echo "🔍 Verifying connections..."

# Check Kafka
if curl -s -f http://localhost:9092 >/dev/null 2>&1; then
    echo "  ✅ Kafka connection: OK"
else
    echo "  ❌ Kafka connection: FAILED"
    echo "  🔧 Troubleshoot: Check 'kubectl get pods -n dev | grep kafka'"
fi

# Check MySQL
if netstat -an | grep :3306 >/dev/null 2>&1; then
    echo "  ✅ MySQL port-forward: OK"
else
    echo "  ❌ MySQL port-forward: FAILED"
    echo "  🔧 Troubleshoot: Check 'kubectl get pods -n qc | grep mysql'"
fi

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
