#!/usr/bin/env python3
"""
Complete CDC Monitor - ALL QC Databases
Monitors ALL database changes from ALL QC databases
Captures notes, agents, activities, documents, etc.
"""

import mysql.connector
import json
import time
import threading
from datetime import datetime
from kafka import KafkaProducer
import logging
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompleteCDCMonitor:
    def __init__(self):
        """Initialize Complete CDC Monitor"""
        logger.info("🔥 Complete CDC Monitor (ALL QC Databases) initialized")
        
        # Database configuration - QC environment
        self.db_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'Gdwedfkndgwodn@123'
        }
        
        # Binlog reader configuration - MONITOR ALL QC DATABASES
        self.binlog_config = {
            'connection_settings': {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'passwd': 'Gdwedfkndgwodn@123'
            },
            'server_id': 3001,  # Unique server ID
            'only_events': [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            'only_schemas': [
                'omre_cbp_activity_qc',        # Notes, tasks, events
                'omre_cbp_agent_core_qc',      # Agents
                'omre_cbp_audit_log_qc',       # Audit logs
                'omre_cbp_calendar_qc',        # Calendar events
                'omre_cbp_collab_qc',          # Collaboration
                'omre_cbp_document_service_qc', # Documents
                'omre_cbp_listing_qc',         # Property listings
                'omre_cbp_transaction_qc'      # Transactions
            ],
            # NO only_tables filter = monitor ALL tables in ALL schemas
            'blocking': True,
            'resume_stream': True
        }
        
        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': ['kafka.qc.svc.cluster.local:9092'],
            'value_serializer': lambda x: json.dumps(x, default=str).encode('utf-8')
        }
        
        self.raw_topic = 'omre-cbp-cdp-raw-test-qc'
        self.kafka_producer = None
        self.stream = None
        self.is_monitoring = False
    
    def connect_kafka(self):
        """Connect to Kafka producer"""
        try:
            self.kafka_producer = KafkaProducer(**self.kafka_config)
            logger.info("✅ Connected to Kafka producer")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def serialize_data(self, data):
        """Convert data to JSON-serializable format"""
        if data is None:
            return None
        result = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif value is None:
                result[key] = None
            else:
                result[key] = str(value) if not isinstance(value, (int, float, bool, str)) else value
        return result
    
    def create_debezium_message(self, operation, before_data, after_data, table_name, schema):
        """Create Debezium-style CDC message from real database change"""
        ts_ms = int(time.time() * 1000)
        
        message = {
            "payload": {
                "op": operation,  # c=create, u=update, d=delete
                "source": {
                    "db": schema,
                    "table": table_name,
                    "ts_ms": ts_ms,
                    "lsn": str(ts_ms),
                    "snapshot": "false"
                },
                "before": self.serialize_data(before_data),
                "after": self.serialize_data(after_data),
                "ts_ms": ts_ms
            }
        }
        
        return message
    
    def get_record_id(self, data, table_name, schema):
        """Extract record ID from data based on table structure"""
        if data is None:
            return "unknown"
        
        # Common ID field patterns by schema and table
        if schema == 'omre_cbp_activity_qc':
            if table_name == 'tasks':
                return data.get('task_id', 'unknown')
            elif table_name == 'events':
                return data.get('event_id', 'unknown')
        elif schema == 'omre_cbp_agent_core_qc':
            if table_name == 'agents':
                return data.get('id', 'unknown')
        
        # Generic ID field search
        id_fields = ['id', 'task_id', 'event_id', 'agent_id', 'user_id', 'document_id']
        
        for field in id_fields:
            if field in data and data[field] is not None:
                return data[field]
        
        # If no standard ID found, use first field that looks like an ID
        for key, value in data.items():
            if key.endswith('_id') and value is not None:
                return value
        
        return "unknown"
    
    def send_cdc_message(self, message, schema, table_name, record_id):
        """Send CDC message to Kafka"""
        try:
            key = f"{schema}.{table_name}_{record_id}".encode('utf-8')
            future = self.kafka_producer.send(self.raw_topic, key=key, value=message)
            result = future.get(timeout=10)
            logger.info(f"🚀 CDC sent: {schema}.{table_name}.{record_id} → partition={result.partition}, offset={result.offset}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send CDC for {schema}.{table_name}.{record_id}: {e}")
            return False
    
    def start_monitoring(self):
        """Start real-time monitoring of ALL database changes"""
        logger.info("🔍 Starting COMPLETE database monitoring...")
        logger.info("🌍 Watching for ALL changes in ALL QC databases")
        logger.info("📋 Monitoring: activity, agent_core, audit_log, calendar, collab, document_service, listing, transaction")
        
        try:
            # Create binlog stream reader
            self.stream = BinLogStreamReader(**self.binlog_config)
            self.is_monitoring = True
            
            logger.info("✅ Binlog stream established - monitoring ALL QC databases...")
            logger.info("💡 Perform any action on web - notes, agents, documents, etc.!")
            
            for binlogevent in self.stream:
                if not self.is_monitoring:
                    break
                
                table_name = binlogevent.table
                schema_name = binlogevent.schema
                
                # Process different types of events
                if isinstance(binlogevent, WriteRowsEvent):
                    # INSERT operation
                    for row in binlogevent.rows:
                        record_id = self.get_record_id(row['values'], table_name, schema_name)
                        logger.info(f"🔥 REAL INSERT: {schema_name}.{table_name} ID={record_id}")
                        
                        # Show key data for important tables
                        if table_name == 'tasks':
                            task_name = row['values'].get('name', 'Unknown')
                            logger.info(f"   📝 Task: {task_name}")
                        elif table_name == 'agents':
                            agent_name = row['values'].get('name', 'Unknown')
                            logger.info(f"   👤 Agent: {agent_name}")
                        
                        cdc_message = self.create_debezium_message('c', None, row['values'], table_name, schema_name)
                        self.send_cdc_message(cdc_message, schema_name, table_name, record_id)
                
                elif isinstance(binlogevent, UpdateRowsEvent):
                    # UPDATE operation
                    for row in binlogevent.rows:
                        record_id = self.get_record_id(row['after_values'], table_name, schema_name)
                        logger.info(f"🔄 REAL UPDATE: {schema_name}.{table_name} ID={record_id}")
                        
                        # Show what changed for important fields
                        if table_name == 'tasks':
                            old_comments = row['before_values'].get('comments', '')
                            new_comments = row['after_values'].get('comments', '')
                            if old_comments != new_comments:
                                logger.info(f"   💬 Comments changed: '{old_comments}' → '{new_comments}'")
                        
                        cdc_message = self.create_debezium_message('u', row['before_values'], row['after_values'], table_name, schema_name)
                        self.send_cdc_message(cdc_message, schema_name, table_name, record_id)
                
                elif isinstance(binlogevent, DeleteRowsEvent):
                    # DELETE operation
                    for row in binlogevent.rows:
                        record_id = self.get_record_id(row['values'], table_name, schema_name)
                        logger.info(f"🗑️ REAL DELETE: {schema_name}.{table_name} ID={record_id}")
                        
                        cdc_message = self.create_debezium_message('d', row['values'], None, table_name, schema_name)
                        self.send_cdc_message(cdc_message, schema_name, table_name, record_id)
                
        except KeyboardInterrupt:
            logger.info("🛑 Monitoring stopped by user")
        except Exception as e:
            logger.error(f"❌ Monitoring error: {e}")
        finally:
            self.stop_monitoring()
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_monitoring = False
        if self.stream:
            self.stream.close()
        logger.info("⏹️ Database monitoring stopped")
    
    def close(self):
        """Close connections"""
        self.stop_monitoring()
        if self.kafka_producer:
            self.kafka_producer.close()
        logger.info("🔌 Connections closed")

def main():
    """Main function"""
    monitor = CompleteCDCMonitor()
    
    try:
        if not monitor.connect_kafka():
            print("❌ Failed to connect to Kafka")
            return
        
   
        
        # Start monitoring ALL QC databases
        monitor.start_monitoring()
        
    except KeyboardInterrupt:
        logger.info("🛑 Monitor interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        monitor.close()

if __name__ == "__main__":
    main() 
