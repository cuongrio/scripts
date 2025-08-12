#!/bin/bash

# 🛑 Kafka CDC Pipeline Stop Script

echo "🛑 Stopping Kafka CDC Pipeline..."

# Stop CDC processes
echo "🔄 Stopping CDC processes..."
pkill -f "complete_cdc_monitor" 2>/dev/null && echo "  ✅ CDC Monitor stopped" || echo "  ℹ️  CDC Monitor was not running"
pkill -f "activity_event_pipeline" 2>/dev/null && echo "  ✅ Activity Pipeline stopped" || echo "  ℹ️  Activity Pipeline was not running"

# Stop port-forwards
echo "🌉 Stopping port-forwards..."
pkill -f "kubectl port-forward.*kafka" 2>/dev/null && echo "  ✅ Kafka port-forward stopped" || echo "  ℹ️  Kafka port-forward was not running"
pkill -f "kubectl port-forward.*mysql" 2>/dev/null && echo "  ✅ MySQL port-forward stopped" || echo "  ℹ️  MySQL port-forward was not running"

# Wait for cleanup
sleep 2

# Verify everything is stopped
echo "🔍 Verifying cleanup..."

CDC_RUNNING=$(ps aux | grep -E "(complete_cdc_monitor|activity_event_pipeline)" | grep python | grep -v grep | wc -l)
PORTFORWARD_RUNNING=$(ps aux | grep "kubectl port-forward" | grep -v grep | wc -l)

if [ $CDC_RUNNING -eq 0 ]; then
    echo "  ✅ All CDC processes stopped"
else
    echo "  ⚠️  Some CDC processes still running ($CDC_RUNNING found)"
    ps aux | grep -E "(complete_cdc_monitor|activity_event_pipeline)" | grep python | grep -v grep
fi

if [ $PORTFORWARD_RUNNING -eq 0 ]; then
    echo "  ✅ All port-forwards stopped"
else
    echo "  ⚠️  Some port-forwards still running ($PORTFORWARD_RUNNING found)"
    ps aux | grep "kubectl port-forward" | grep -v grep
fi

echo ""
echo "🎯 Pipeline stop complete!"
echo "📊 Check status: ps aux | grep -E '(complete_cdc_monitor|activity_event_pipeline|kubectl port-forward)' | grep -v grep"
