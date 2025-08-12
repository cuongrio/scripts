# 🚀 Kafka CDC Message Pipeline

## 📋 Mô tả
Pipeline CDC (Change Data Capture) để parse messages từ Debezium và gửi messages có nghĩa sang clean Kafka topic.

## 📁 Files

### Core Scripts
- `complete_cdc_monitor.py` - CDC Monitor bắt tất cả QC databases, gửi raw CDC messages
- `activity_event_pipeline.py` - Parse raw messages, tạo meaningful messages gửi sang clean topic
- `schema_cache_activity.json` - Cache mapping UNKNOWN_COLx fields với real column names

## 🔧 Cài đặt

### 1. Dependencies
```bash
pip install -r requirements.txt
```

### 2. Kiểm tra gcloud authentication
```bash
gcloud auth list
gcloud config get-value project
```

### 3. Get k8s credentials
```bash
gcloud container clusters get-credentials vinid-cluster --zone asia-southeast1-a --project vinid-data-warehouse
```

## 🚦 Chạy Pipeline

### AUTO SCRIPT (Khuyến nghị)
```bash
./start_pipeline.sh    # Start tất cả
./stop_pipeline.sh     # Stop tất cả
```

### MANUAL SETUP

#### BƯỚC 1: Setup Port Forwarding (QUAN TRỌNG!)

**a) Kafka (dev namespace):**
```bash
kubectl port-forward services/kafka 9092:9092 -n dev &
```

**b) MySQL (qc namespace):**
```bash
kubectl port-forward services/mysql 3306:3306 -n qc &
```

**Verify connections:**
```bash
# Check Kafka
curl -f http://localhost:9092 || echo "Kafka not ready"

# Check MySQL
netstat -an | grep :3306
```

#### BƯỚC 2: Clear Proxy (nếu có lỗi connection)
```bash
unset HTTPS_PROXY
unset HTTP_PROXY
```

#### BƯỚC 3: Chạy CDC Monitor
```bash
python3 complete_cdc_monitor.py &
```

#### BƯỚC 4: Chạy Activity Pipeline  
```bash
python3 activity_event_pipeline.py &
```

#### BƯỚC 5: Verify
```bash
ps aux | grep -E "(complete_cdc_monitor|activity_event_pipeline)" | grep python
```

## 📨 Message Formats

### 1. UPDATE-EVENT (Activity Events)
**Kafka Key:** `update-event`
```json
{
  "event": "update-event",
  "payload": {
    "id": "13767cf3-d343-41b5-a9c9-f4515f69579f",
    "type": "CUSTOMER",
    "entity_type": "CUSTOMER",
    "entity_id": "1f2cdf46-b678-4a89-b3f9-58fea40b49de",
    "entity_code": "",
    "entity_name": "",
    "parent_id": "eeec12e3-cb5e-40d8-a570-48646bba5a9c",
    "parent_type": "TASK",
    "created_by": "1000056",
    "created_date": "2025-08-11T14:16:00",
    "last_modified_by": "1000056",
    "last_modified_date": "2025-08-11T14:16:00",
    "ts_ms": 1754896560446,
    "agent_id": "1000056",
    "contact_id": "1f2cdf46-b678-4a89-b3f9-58fea40b49de",
    "task_id": "eeec12e3-cb5e-40d8-a570-48646bba5a9c",
    "action": "create",
    "verified": true,
    "task_type_code": "deposit5629",
    "start_date": "2026-01-17T00:00:00"
  }
}
```

### 2. UPDATE-PROFILE (Customer Creation)
**Kafka Key:** `update-profile`
```json
{
  "event": "update-profile",
  "payload": {
    "contact_id": "097c271c-c753-4e5e-9f58-7687581970a8",
    "basic_info": {
      "contact_id": "097c271c-c753-4e5e-9f58-7687581970a8",
      "created_date": "2025-08-11T14:36:00.109000",
      "last_modified_date": "2025-08-11T14:36:00.109000",
      "created_by": "1000056",
      "last_modified_by": "1000056",
      "status": "ACTIVE",
      "verification_status": "NOT_VERIFIED",
      "type": null,
      "full_name": "Khách hàng 2",
      "source": "AGENT",
      "channel": "AGENT_WEB",
      "note": "123123123",
      "last_interaction_date": null
    },
    "profile_details": {
      "contact_profile_id": "83a5636e-4dbb-4157-b880-8736e49ae77d",
      "created_date": "2025-08-11T14:36:00.204000",
      "last_modified_date": "2025-08-11T14:36:00.204000",
      "created_by": "1000056",
      "last_modified_by": "1000056",
      "contact_id": "097c271c-c753-4e5e-9f58-7687581970a8",
      "gender": "FEMALE",
      "nationality": null,
      "company": "Làm việc",
      "job_type": "central_government",
      "job_position": "STAFF",
      "marital_status": null,
      "vehicle": null,
      "owned_property": null,
      "property_type": null,
      "mortgage_need": null,
      "date_of_birth": "2025-08-14T00:00:00",
      "number_of_children": null,
      "children_school": null,
      "children_name": null,
      "total_assets": null,
      "social_profiles": "[]",
      "total_income": null
    },
    "emails": {
      "email_id": "58836f12-3a70-4cee-a34f-54e9a4fd7843",
      "created_date": "2025-08-11T14:36:00.114000",
      "last_modified_date": "2025-08-11T14:36:00.114000",
      "created_by": "1000056",
      "last_modified_by": "1000056",
      "contact_id": "097c271c-c753-4e5e-9f58-7687581970a8",
      "email": "test.10121212@gmail.com",
      "deleted": 0
    },
    "phone_numbers": {
      "phone_number_id": "80dba0d6-d3aa-468f-9e70-75f9106dcacf",
      "created_date": "2025-08-11T14:36:00.112000",
      "last_modified_date": "2025-08-11T14:36:00.112000",
      "created_by": "1000056",
      "last_modified_by": "1000056",
      "contact_id": "097c271c-c753-4e5e-9f58-7687581970a8",
      "number": "0844118922",
      "deleted": 0
    },
    "addresses": {
      "address_id": "dfa9b680-4814-4f6c-a195-b5ea47983610",
      "created_date": "2025-08-11T14:36:00.213000",
      "last_modified_date": "2025-08-11T14:36:00.213000",
      "created_by": "1000056",
      "last_modified_by": "1000056",
      "deleted": 0,
      "contact_id": "097c271c-c753-4e5e-9f58-7687581970a8",
      "type": "CURRENT_ADDRESS",
      "province_code": null,
      "province_name": null,
      "district_code": null,
      "district_name": null,
      "ward_code": null,
      "ward_name": null,
      "address_number": "123123",
      "address_detail": "123123"
    },
    "id_documents": {},
    "event_type": "update-profile",
    "created_by": "1000056",
    "created_at": "2025-08-11T14:36:00.109000",
    "complete_profile": true
  }
}
```

### 3. DELETE-PROFILE (Customer Deletion)
**Kafka Key:** `delete-profile`
```json
{
  "event": "delete-profile",
  "payload": {
    "agent_id": "1000056",
    "contact_id": "808f7178-14c3-4239-ab69-8b95ff5808ed",
    "last_modified_date": "2025-08-11T14:42:54.522000",
    "created_by": "1000056",
    "last_modified_by": "1000056",
    "ts_ms": 1754898175262
  }
}
```

## ⚠️ Troubleshooting

### 1. Lỗi Kafka Connection (ECONNREFUSED)
```bash
# Kill existing port-forwards
pkill -f "kubectl port-forward.*kafka"

# Clear proxy
unset HTTPS_PROXY
unset HTTP_PROXY

# Re-establish port-forward
kubectl port-forward services/kafka 9092:9092 -n dev &

# Wait và test
sleep 5
curl -f http://localhost:9092 || echo "Still not ready"
```

### 2. Lỗi MySQL Connection
```bash
# Kill existing MySQL port-forwards
pkill -f "kubectl port-forward.*mysql"

# Re-establish to QC namespace (QUAN TRỌNG!)
kubectl port-forward services/mysql 3306:3306 -n qc &

# Verify
netstat -an | grep :3306
```

### 3. Không nhận CDC messages
```bash
# Check pipelines đang chạy
ps aux | grep python | grep -E "(complete_cdc_monitor|activity_event_pipeline)"

# Kill và restart
pkill -f "complete_cdc_monitor"
pkill -f "activity_event_pipeline"

# Start lại
python3 complete_cdc_monitor.py &
python3 activity_event_pipeline.py &
```

### 4. Pipeline gửi sai format messages
- **Check:** Chỉ có 3 loại message được gửi: update-event, update-profile, delete-profile
- **Fix:** Restart pipeline, kiểm tra logs để xem tables nào đang được process

## 🎯 Tính năng chính

### Message Aggregation
- **Profile Events:** Combine data từ `cont_contacts`, `cont_contact_profiles`, `cont_emails`, `cont_phone_numbers`, `cont_addresses`, `cont_id_documents`
- **Activity Events:** Combine data từ `relations` và `tasks` tables
- **Timeout:** 2.0s cho profile, 3.0s cho activity
- **1 message per action:** Mỗi hành động chỉ gửi 1 message tổng hợp

### Smart Filtering  
- **Profile tables:** CHỈ process 6 tables listed above, skip tất cả tables khác
- **Activity tables:** CHỈ process `relations` và `tasks`, skip tất cả tables khác
- **Database filtering:** `omre_cbp_activity_qc` (activity) và `omre_cbp_collab_qc` (profile)

### Schema Mapping
- Automatic mapping `UNKNOWN_COLx` → real column names
- Cache schema to avoid DB lookup overhead
- Support multiple databases và tables

## 📊 Topics

### Raw Topic
- **Name:** `raw-topic`
- **Content:** Debezium CDC messages từ tất cả QC databases

### Clean Topic  
- **Name:** `clean-topic`
- **Content:** Meaningful messages với 3 formats (update-event, update-profile, delete-profile)
- **Headers:** `{"__TypeId__": "net.vinid.core.event.EventMessage"}`

---

## 🚀 Quick Start
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start pipeline (auto setup)
./start_pipeline.sh

# 3. Stop pipeline
./stop_pipeline.sh
```

**Pipeline sẵn sàng để capture và process CDC messages!** 🎉
