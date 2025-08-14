#!/bin/bash

set -e

echo "🛠️ Cập nhật hệ thống và cài đặt Python3, pip..."
apt-get update -y
apt-get install -y python3 python3-pip

echo "📦 Cài đặt kafka-python==2.0.2, pymysql==1.0.2 và mysql-connector-python..."
pip3 install kafka-python==2.0.2 pymysql==1.0.2 mysql-connector-python

echo "🧪 Tạo file kiểm tra kết nối MySQL và Kafka..."

cat << 'EOF' > test_connections.py
#!/usr/bin/env python3
import mysql.connector
from kafka import KafkaProducer

def test_mysql():
    try:
        conn = mysql.connector.connect(
            host='mysql.qc.svc.cluster.local',
            user='root',
            password='Gdwedfkndgwodn@123',
            port=3306,
            charset='utf8mb4'
        )
        if conn.is_connected():
            print("✅ Kết nối MySQL thành công!")
            conn.close()
        else:
            print("❌ Kết nối MySQL thất bại!")
    except Exception as e:
        print(f"❌ Lỗi kết nối MySQL: {e}")

def test_kafka():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka.dev.svc.cluster.local:9092'],
            client_id='test-connection'
        )
        producer.close()
        print("✅ Kết nối Kafka thành công!")
    except Exception as e:
        print(f"❌ Lỗi kết nối Kafka: {e}")

if __name__ == "__main__":
    test_mysql()
    test_kafka()
EOF

echo "▶️ Chạy kiểm tra kết nối..."
python3 test_connections.py

echo "✅ Hoàn thành cài đặt và kiểm tra kết nối."
