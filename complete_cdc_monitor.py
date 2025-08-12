#!/usr/bin/env python3
"""
Complete CDC Monitor - ALL QC Databases (Internal Cluster Version)
"""

import mysql.connector
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompleteCDCMonitor:
    def __init__(self):
        """Initialize Complete CDC Monitor"""
        logger.info("🔥 Complete CDC Monitor (QC Internal) initialized")
        
        # MySQL internal DNS config
        self.db_host = "mysql.qc.svc.cluster.local"
        self.db_port = 3306
        self.db_user = "root"
        self.db_password = "Gdwedfkndgwodn@123"

        self.db_config = {
            'host': self.db_host,
            'port': self.db_port,
            'user': self.db_user,
            'password': self.db_password
        }
        
        # Binlog configuration
        self.binlog_config = {
            'connection_settings': {
                'host': self.db_host,
                'port': self.db_port,
                'user': self.db_user,
                'passwd': self.db_password
            },
            'server_id': 3001,
            'only_events': [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            'only_schemas': [
                'omre_cbp_activity_qc',
                'omre_cbp_agent_core_qc',
                'omre_cbp_audit_log_qc',
                'omre_cbp_calendar_qc',
                'omre_cbp_collab_qc',
                'omre_cbp_document_service_qc',
                'omre_cbp_listing_qc',
                'omre_cbp_transaction_qc'
            ],
            'blocking': True,
            'resume_stream': True
        }
        
        # Kafka internal DNS config
        self.kafka_config = {
            'bootstrap_servers': ['kafka.qc.svc.cluster.local:9092'],
            'value_serializer': lambda x: json.dumps(x, default=str).encode('utf-8')
        }
        
        self.raw_topic = 'omre-cbp-cdp-raw-test-dev'
        self.kafka_producer = None
        self.stream = None
        self.is_monitoring = False

    def connect_kafka(self):
        try:
            self.kafka_producer = KafkaProducer(**self.kafka_config)
            logger.info("✅ Connected to Kafka producer")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False

    def serialize_data(self, data):
        if data is None:
            return None
        result = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result

    def create_debezium_message(self, operation, before_data, after_data, table_name, schema):
        ts_ms = int(time.time() * 1000)
        return {
            "payload": {
                "op": operation,
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

    def get_record_id(self, data, table_name, schema):
        if not data:
            return "unknown"
        id_fields = ['id', 'task_id', 'event_id', 'agent_id', 'user_id', 'document_id']
        for field in id_fields:
            if field in data and data[field] is not None:
                return data[field]
        for key, value in data.items():
            if key.endswith('_id') and value is not None:
                return value
        return "unknown"

    def send_cdc_message(self, message, schema, table_name, record_id):
        try:
            key = f"{schema}.{table_name}_{record_id}".encode('utf-8')
            future = self.kafka_producer.send(self.raw_topic, key=key, value=message)
            result = future.get(timeout=10)
            logger.info(f"🚀 CDC sent: {schema}.{table_name}.{record_id} → partition={result.partition}, offset={result.offset}")
        except Exception as e:
            logger.error(f"❌ Failed to send CDC: {e}")

    def start_monitoring(self):
        try:
            self.stream = BinLogStreamReader(**self.binlog_config)
            self.is_monitoring = True
            logger.info("✅ Binlog stream established - monitoring QC databases...")
            for binlogevent in self.stream:
                if not self.is_monitoring:
                    break
                schema_name = binlogevent.schema
                table_name = binlogevent.table

                if isinstance(binlogevent, WriteRowsEvent):
                    op = 'c'
                    for row in binlogevent.rows:
                        record_id = self.get_record_id(row['values'], table_name, schema_name)
                        cdc_message = self.create_debezium_message(op, None, row['values'], table_name, schema_name)
                        self.send_cdc_message(cdc_message, schema_name, table_name, record_id)

                elif isinstance(binlogevent, UpdateRowsEvent):
                    op = 'u'
                    for row in binlogevent.rows:
                        record_id = self.get_record_id(row['after_values'], table_name, schema_name)
                        cdc_message = self.create_debezium_message(op, row['before_values'], row['after_values'], table_name, schema_name)
                        self.send_cdc_message(cdc_message, schema_name, table_name, record_id)

                elif isinstance(binlogevent, DeleteRowsEvent):
                    op = 'd'
                    for row in binlogevent.rows:
                        record_id = self.get_record_id(row['values'], table_name, schema_name)
                        cdc_message = self.create_debezium_message(op, row['values'], None, table_name, schema_name)
                        self.send_cdc_message(cdc_message, schema_name, table_name, record_id)
        finally:
            self.stop_monitoring()

    def stop_monitoring(self):
        self.is_monitoring = False
        if self.stream:
            self.stream.close()
        logger.info("⏹️ Monitoring stopped")

    def close(self):
        self.stop_monitoring()
        if self.kafka_producer:
            self.kafka_producer.close()

def main():
    monitor = CompleteCDCMonitor()
    if not monitor.connect_kafka():
        return
    try:
        monitor.start_monitoring()
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
    finally:
        monitor.close()

if __name__ == "__main__":
    main()
