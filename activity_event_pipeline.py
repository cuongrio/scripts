#!/usr/bin/env python3
"""
Activity Event Pipeline
Parses raw CDC messages into simplified activity event structure
Only includes available fields, skips missing ones
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import pymysql
import time
import threading
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ActivityEventPipeline:
    def __init__(self):
        """Initialize Activity Event Pipeline"""
        logger.info("🚀 Activity Event Pipeline initialized")
        
        # Kafka configuration - Internal cluster DNS
        self.kafka_config = {
            'bootstrap_servers': ['kafka.qc.svc.cluster.local:9092'],
            'auto_offset_reset': 'latest',  # Only process new messages
            'enable_auto_commit': True,
            'group_id': 'activity-event-processor',
            'value_deserializer': lambda x: json.loads(x.decode('utf-8'))
        }
        
        self.producer_config = {
            'bootstrap_servers': ['kafka.qc.svc.cluster.local:9092'],
            'value_serializer': lambda x: json.dumps(x, default=str).encode('utf-8')
        }
        
        self.raw_topic = 'omre-cbp-cdp-raw-test-qc'
        self.cleaned_topic = 'omre-cbp-cdp-cleaned-test-qc'
        
        # MySQL connection settings for metadata lookup (QC) - Internal cluster DNS
        self.mysql_config = {
            'host': 'mysql.qc.svc.cluster.local',
            'port': 3306,
            'user': 'root',
            'password': 'Gdwedfkndgwodn@123',
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor,
        }
        # Cache of (db, table) -> [column_name ordered by ordinal]
        self._columns_cache: Dict[str, List[str]] = {}
        # Streaming correlation store (in-memory, TTL) for relations↔tasks
        self._relation_by_task: Dict[str, Dict[str, Any]] = {}
        self._relation_ttl_seconds: int = 300
        # Cache tasks for enrichment (in-memory, TTL)
        self._task_by_id: Dict[str, Dict[str, Any]] = {}
        # Disable DB lookups for stream-only mode (override one-time with ENABLE_DB_LOOKUP=1)
        self.enable_db_lookup: bool = bool(int(os.getenv('ENABLE_DB_LOOKUP', '0')))
        # Persisted schema cache path
        self._schema_cache_file = os.path.join(os.path.dirname(__file__), 'schema_cache_activity.json')
        self._load_schema_cache_from_disk()
        
        # Message aggregation for profile events
        self.profile_message_buffer = defaultdict(dict)  # contact_id -> {messages by table}
        self.profile_timers = {}  # contact_id -> timer
        self.aggregation_timeout = 3.0  # seconds to wait for related messages
        self.buffer_lock = threading.Lock()
        
        # Message aggregation for activity events  
        self.activity_message_buffer = defaultdict(dict)  # task_id -> {messages by table}
        self.activity_timers = {}  # task_id -> timer
        self.activity_timeout = 7.0  # timeout for activity events aggregation
        
        self.consumer = None
        self.producer = None
    
    def connect(self):
        """Connect to Kafka consumer and producer"""
        try:
            self.consumer = KafkaConsumer(self.raw_topic, **self.kafka_config)
            self.producer = KafkaProducer(**self.producer_config)
            logger.info("✅ Connected to Kafka consumer and producer")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def _columns_cache_key(self, db: str, table: str) -> str:
        return f"{db}.{table}".lower()

    def _load_schema_cache_from_disk(self) -> None:
        try:
            if os.path.exists(self._schema_cache_file):
                with open(self._schema_cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    for k, v in data.items():
                        if isinstance(v, list):
                            self._columns_cache[k] = v
                logger.info(f"📚 Loaded schema cache from disk: {len(self._columns_cache)} tables")
        except Exception as e:
            logger.warning(f"⚠️ Failed to load schema cache: {e}")

    def _save_schema_cache_to_disk(self) -> None:
        try:
            with open(self._schema_cache_file, 'w', encoding='utf-8') as f:
                json.dump(self._columns_cache, f, ensure_ascii=False, indent=2)
            logger.info("💾 Schema cache saved")
        except Exception as e:
            logger.warning(f"⚠️ Failed to save schema cache: {e}")

    def _get_columns_for_table(self, db: str, table: str) -> List[str]:
        """Fetch ordered column names for a table from information_schema and cache them."""
        cache_key = self._columns_cache_key(db, table)
        if cache_key in self._columns_cache:
            return self._columns_cache[cache_key]
        cols: List[str] = []
        if self.enable_db_lookup:
            try:
                conn = pymysql.connect(**self.mysql_config)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COLUMN_NAME
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ORDINAL_POSITION
                        """,
                        (db, table),
                    )
                    rows = cur.fetchall()
                    cols = [r['COLUMN_NAME'] for r in rows]
                conn.close()
                if not cols:
                    logger.warning(f"⚠️ Column lookup returned empty for {db}.{table}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to fetch columns for {db}.{table}: {e}")
        self._columns_cache[cache_key] = cols
        if cols:
            self._save_schema_cache_to_disk()
        return cols

    def _map_unknown_columns(self, row: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        """Replace UNKNOWN_COLx keys with actual column names by ordinal index when possible."""
        if not row:
            return row
        mapped: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(key, str) and key.startswith('UNKNOWN_COL'):
                try:
                    idx = int(key.replace('UNKNOWN_COL', ''))
                except ValueError:
                    idx = None
                if idx is not None and 0 <= idx < len(columns):
                    mapped_key = columns[idx]
                else:
                    mapped_key = key
            else:
                mapped_key = key
            mapped[mapped_key] = value
        return mapped

    def _compute_changed_columns(self, before: Dict[str, Any], after: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compute list of changed columns with before/after values."""
        if before is None:
            before = {}
        if after is None:
            after = {}
        changes: List[Dict[str, Any]] = []
        all_keys = set(before.keys()) | set(after.keys())
        for col in sorted(all_keys):
            if before.get(col) != after.get(col):
                changes.append({
                    'column': col,
                    'before': before.get(col),
                    'after': after.get(col),
                })
        return changes

    def _extract_actor_ids(self, operation: str, table: str, before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
        """Extract actor identifiers from activity rows and format OMRE_/CBPA_ ids.
        Preference order per op: update → updated_by_id/last_modified_by_id; create → created_by_id; delete → deleted_by_id/last_modified_by_id.
        Fallbacks include common *_agent_id, user_id, owner_id, assignee_id.
        """
        candidates_update = [
            'updated_by_id', 'last_modified_by_id', 'modified_by_id',
            'updated_by', 'last_modified_by',
        ]
        candidates_create = [
            'created_by_id', 'creator_id', 'created_by',
        ]
        candidates_delete = [
            'deleted_by_id', 'deleted_by', 'last_modified_by_id',
        ]
        candidates_common = [
            'agent_id', 'performer_id', 'actor_id', 'user_id', 'owner_id', 'assignee_id', 'executor_id',
            'assigned_agent_id', 'responsible_agent_id'
        ]
        def pick_value(keys: List[str], src: Dict[str, Any]) -> Any:
            for k in keys:
                if k in src and src[k] not in (None, ''):
                    return src[k]
            return None
        actor_val = None
        if operation == 'u':
            actor_val = pick_value(candidates_update, after) or pick_value(candidates_update, before)
        elif operation == 'c':
            actor_val = pick_value(candidates_create, after)
        elif operation == 'd':
            actor_val = pick_value(candidates_delete, before)
        if actor_val is None:
            actor_val = pick_value(candidates_common, after) or pick_value(candidates_common, before)
        # Normalize to string id if possible
        try:
            actor_str = str(actor_val).strip()
        except Exception:
            actor_str = None
        actor: Dict[str, Any] = {}
        if actor_str:
            actor['omre_agent_id'] = f"{actor_str}"
            actor['cbp_agent_id'] = f"{actor_str}"
        return actor
 
    def _existing_table_columns(self, db: str, table: str) -> List[str]:
        """Return cached list of existing columns for a table."""
        return self._get_columns_for_table(db, table)

    def _pick_table_id(self, table: str, row: Dict[str, Any]):
        """Heuristically choose the identifier column/value for a row.
        Returns (id_col, id_val) or (None, None).
        """
        if not row:
            return None, None
        candidates = [
            'id', f"{table.rstrip('s')}_id", 'task_id', 'event_id', 'note_id', 'activity_id', 'record_id'
        ]
        for k in candidates:
            if k in row and row[k] not in (None, ''):
                return k, row[k]
        for k, v in row.items():
            if isinstance(k, str) and k.endswith('_id') and v not in (None, ''):
                return k, v
        return None, None

    def _pick_agent_from_row(self, row: Dict[str, Any]):
        """Pick agent id from a row using common keys."""
        if not row:
            return None
        for k in ['agent_id', 'assigned_agent_id', 'responsible_agent_id', 'user_id', 'owner_id', 'assignee_id']:
            if k in row and row[k] not in (None, ''):
                return row[k]
        return None
 
    def _extract_relation_link(self, operation: str, after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
        """Derive agent-contact link from streaming 'relations' records.
        Mapping per requirement:
          - parent_id => task_id
          - created_by => omre_agent_id (actor)
          - entity_id => contact_id (customer)
        Also include action derived from op: c/u/d.
        """
        src = after if operation != 'd' else before
        if not isinstance(src, dict):
            return {}
        task_id = src.get('parent_id') or src.get('task_id')
        agent_id = src.get('created_by') or src.get('updated_by') or src.get('user_id')
        contact_id = src.get('entity_id') or src.get('contact_id') or src.get('customer_id')
        action_map = {'c': 'create', 'u': 'update', 'd': 'delete'}
        action = action_map.get(operation, 'unknown')
        result: Dict[str, Any] = {}
        if agent_id not in (None, ''):
            result['agent_id'] = str(agent_id)
        if contact_id not in (None, ''):
            result['contact_id'] = str(contact_id)
        if task_id not in (None, ''):
            result['task_id'] = str(task_id)
        result['action'] = action
        result['verified'] = True
        return {k: v for k, v in result.items() if v not in (None, '')}
 
    def _verify_agent_contact_link(
        self,
        db: str,
        table: str,
        row_after: Dict[str, Any],
        row_before: Dict[str, Any],
        contact_id: Any,
        actor: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Verify agent-contact relationship by checking the current row in DB if possible.
        Returns a compact object with agent_id, contact_id and verified flag.
        Always safe; never raises to caller.
        """
        result: Dict[str, Any] = {}
        # Initial candidates from payload
        agent_from_row = self._pick_agent_from_row(row_after) or self._pick_agent_from_row(row_before)
        agent_from_actor = None
        if actor and isinstance(actor, dict):
            agent_from_actor = actor.get('omre_agent_id') or actor.get('cbp_agent_id')
        # Normalize IDs to strings
        def norm(v):
            try:
                return str(v).strip()
            except Exception:
                return None
        agent_row_norm = norm(agent_from_row)
        agent_actor_norm = norm(agent_from_actor)
        contact_norm = norm(contact_id)
        # Prefer concrete agent id from row; else from actor
        chosen_agent = agent_row_norm or agent_actor_norm
        if chosen_agent:
            result['agent_id'] = chosen_agent
        if contact_norm:
            result['contact_id'] = contact_norm
        verified = False
        try:
            # If we have a table id, attempt DB verification
            id_col, id_val = self._pick_table_id(table, row_after or row_before or {})
            if id_col and id_val not in (None, ''):
                cols = self._existing_table_columns(db, table)
                wanted_cols = [c for c in ['agent_id', 'assigned_agent_id', 'responsible_agent_id', 'user_id', 'owner_id', 'assignee_id', 'contact_id', 'customer_id'] if c in cols]
                if wanted_cols:
                    cols_sql = ",".join(f"`{c}`" for c in wanted_cols)
                    sql = f"SELECT {cols_sql} FROM `{db}`.`{table}` WHERE `{id_col}`=%s LIMIT 1"
                    conn = pymysql.connect(**self.mysql_config)
                    try:
                        with conn.cursor() as cur:
                            cur.execute(sql, (id_val,))
                            db_row = cur.fetchone()
                    finally:
                        conn.close()
                    if db_row:
                        # Determine agent and contact from DB row
                        agent_db = None
                        for k in ['agent_id', 'assigned_agent_id', 'responsible_agent_id', 'user_id', 'owner_id', 'assignee_id']:
                            if k in db_row and db_row[k] not in (None, ''):
                                agent_db = db_row[k]
                                break
                        contact_db = None
                        for k in ['contact_id', 'customer_id']:
                            if k in db_row and db_row[k] not in (None, ''):
                                contact_db = db_row[k]
                                break
                        agent_db_norm = norm(agent_db)
                        contact_db_norm = norm(contact_db)
                        # Update chosen values if missing
                        if agent_db_norm and not chosen_agent:
                            result['agent_id'] = agent_db_norm
                            chosen_agent = agent_db_norm
                        if contact_db_norm and not contact_norm:
                            result['contact_id'] = contact_db_norm
                            contact_norm = contact_db_norm
                        # Verification: both present and consistent
                        if chosen_agent and contact_norm:
                            verified = True
        except Exception as e:
            logger.warning(f"⚠️ Agent-contact verification skipped due to error: {e}")
        result['verified'] = bool(verified)
        return result
 
    def _fetch_contact_info(self, contact_id: Any) -> Dict[str, Any]:
        """Fetch minimal contact info from omre_cbp_collab_qc.cont_contacts by contact_id and return only available fields.
        Returns empty dict if not found or on error.
        """
        if contact_id in (None, ""):
            return {}
        cache_key = ("omre_cbp_collab_qc", "cont_contacts", str(contact_id))
        if not hasattr(self, "_contact_cache"):
            self._contact_cache = {}
        if cache_key in self._contact_cache:
            return self._contact_cache[cache_key]
        try:
            columns = self._get_columns_for_table("omre_cbp_collab_qc", "cont_contacts")
            if not columns:
                return {}
            id_col = None
            if 'contact_id' in columns:
                id_col = 'contact_id'
            elif 'id' in columns:
                id_col = 'id'
            else:
                return {}
            # Select a minimal useful set of fields if they exist
            preferred_fields = ['full_name', 'name', 'phone_number', 'phone', 'email', 'zalo']
            select_fields = [id_col] + [c for c in preferred_fields if c in columns]
            cols_sql = ",".join(f"`{c}`" for c in select_fields)
            sql = (
                f"SELECT {cols_sql} FROM `omre_cbp_collab_qc`.`cont_contacts` "
                f"WHERE `{id_col}`=%s LIMIT 1"
            )
            conn = pymysql.connect(**self.mysql_config)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, (contact_id,))
                    row = cur.fetchone()
            finally:
                conn.close()
            if not row:
                result = {'contact_id': str(contact_id)}
            else:
                result = {'contact_id': str(row.get(id_col, contact_id))}
                # Normalize common keys
                name = row.get('full_name') or row.get('name')
                phone = row.get('phone_number') or row.get('phone')
                email = row.get('email')
                zalo = row.get('zalo')
                if name: result['name'] = name
                if phone: result['phone'] = phone
                if email: result['email'] = email
                if zalo: result['zalo'] = zalo
            # Cache and return
            self._contact_cache[cache_key] = result
            return result
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch contact info for {contact_id}: {e}")
            return {}
    
    def parse_debezium_message(self, message_value):
        """Parse Debezium CDC message"""
        try:
            if 'payload' not in message_value:
                return None
            
            payload = message_value['payload']
            operation = payload.get('op', 'unknown')
            source = payload.get('source', {})
            
            # Get data based on operation type
            if operation == 'd':  # delete
                data = payload.get('before', {})
            else:  # create or update
                data = payload.get('after', {})
            
            if data is None:
                data = {}
            
            return {
                'operation': operation,
                'source_db': source.get('db', 'unknown'),
                'source_table': source.get('table', 'unknown'),
                'data': data,
                'timestamp': payload.get('ts_ms', int(datetime.now().timestamp() * 1000)),
                'raw': message_value,  # keep original to pass-through if needed
            }
        except Exception as e:
            logger.error(f"❌ Error parsing Debezium message: {e}")
            return None
    
    def create_activity_event(self, parsed_data):
        """Create activity event with only available fields (legacy path)."""
        operation = parsed_data['operation']
        data = parsed_data['data']
        source_table = parsed_data['source_table']
        
        # Determine event type based on operation and table
        if source_table == 'agents':
            event_map = {
                'c': 'agent_created',
                'u': 'agent_updated', 
                'd': 'agent_deleted'
            }
            event_type = event_map.get(operation, 'agent_activity')
        elif source_table == 'tasks':
            event_map = {
                'c': 'task_created',
                'u': 'task_updated',
                'd': 'task_deleted'
            }
            event_type = event_map.get(operation, 'task_activity')
        else:
            # Generic event for other tables
            event_type = f"{source_table}_{operation}"
        
        # Build payload with ONLY available fields - skip missing fields completely
        payload = {}
        
        # Core identity fields
        if 'id' in data and data['id']:
            payload['omre_agent_id'] = f"OMRE_{data['id']}"
            payload['cbp_agent_id'] = f"CBPA_{data['id']}"
        elif 'task_id' in data and data['task_id']:
            payload['task_id'] = data['task_id']
        
        # Contact information - only add if available
        if 'phone_number' in data and data['phone_number']:
            payload['phone'] = data['phone_number']
            payload['zalo'] = data['phone_number']  # Assume zalo same as phone
        
        if 'email' in data and data['email']:
            payload['email'] = data['email']
        
        # Activity date - only add if available
        if 'last_modified_date' in data and data['last_modified_date']:
            payload['last_active_date'] = str(data['last_modified_date'])[:10]
        elif 'update_time' in data and data['update_time']:
            payload['last_active_date'] = str(data['update_time'])[:10]
        elif 'created_date' in data and data['created_date']:
            payload['last_active_date'] = str(data['created_date'])[:10]
        
        # Activity metrics - only add if data contains them
        if 'monthly_duration' in data and data['monthly_duration']:
            payload['monthly_website_duration_minutes'] = data['monthly_duration']
        
        if 'activity_count' in data and data['activity_count']:
            payload['activity_count_per_contact'] = data['activity_count']
        
        if 'chat_count' in data and data['chat_count']:
            payload['chat_count'] = data['chat_count']
        
        if 'call_count' in data and data['call_count']:
            payload['call_count'] = data['call_count']
        
        if 'task_completion_rate' in data and data['task_completion_rate']:
            payload['task_completion_rate'] = data['task_completion_rate']
        
        if 'listing_count' in data and data['listing_count']:
            payload['total_listing_last_30d'] = data['listing_count']
        
        if 'active_listings' in data and data['active_listings']:
            payload['active_listing_count'] = data['active_listings']
        
        if 'inquiry_count' in data and data['inquiry_count']:
            payload['total_inquiry_count'] = data['inquiry_count']
        
        if 'dealroom_count' in data and data['dealroom_count']:
            payload['total_dealroom_count'] = data['dealroom_count']
        
        if 'agreement_count' in data and data['agreement_count']:
            payload['total_agreement_count'] = data['agreement_count']
        
        # NO DEFAULT VALUES - only send fields that exist in data
        
        return {
            'event': event_type,
            'payload': payload
        }
    
    def _should_send_to_cleaned(self, source_db: str) -> bool:
        """Forward messages for omre_cbp_activity_qc (activity events) and omre_cbp_collab_qc (profile events)."""
        db_lower = (source_db or '').lower()
        return db_lower in ['omre_cbp_activity_qc', 'omre_cbp_collab_qc']
    
    def _flush_profile_buffer(self, contact_id: str):
        """Flush aggregated profile messages and send to cleaned topic"""
        with self.buffer_lock:
            if contact_id not in self.profile_message_buffer:
                return
                
            buffer_data = self.profile_message_buffer[contact_id]
            del self.profile_message_buffer[contact_id]
            
            # Cancel timer if exists
            if contact_id in self.profile_timers:
                self.profile_timers[contact_id].cancel()
                del self.profile_timers[contact_id]
        
        # Determine profile event type
        cont_contacts_msg = buffer_data.get('cont_contacts')
        if not cont_contacts_msg:
            logger.warning(f"No cont_contacts message for contact_id {contact_id}")
            return
            
        operation = cont_contacts_msg.get('operation')
        before = cont_contacts_msg.get('before', {})
        after = cont_contacts_msg.get('after', {})
        
        event_type = None
        if operation == 'c':
            event_type = 'update-profile'
        elif operation == 'u':
            before_status = before.get('status', '').upper()
            after_status = after.get('status', '').upper()
            if before_status == 'ACTIVE' and after_status == 'DELETED':
                event_type = 'delete-profile'
        
        if not event_type:
            return
            
        # Build aggregated payload
        payload = self._build_aggregated_profile_payload(event_type, buffer_data, contact_id)
        if payload:
            self._send_to_cleaned_topic(event_type, payload)
            logger.info(f"✅ Sent aggregated {event_type} for contact_id {contact_id}")
    
    def _add_to_profile_buffer(self, contact_id: str, table_name: str, message_data: Dict):
        """Add message to profile buffer and set/reset timer"""
        with self.buffer_lock:
            self.profile_message_buffer[contact_id][table_name] = message_data
            
            # Cancel existing timer
            if contact_id in self.profile_timers:
                self.profile_timers[contact_id].cancel()
            
            # Set new timer
            timer = threading.Timer(self.aggregation_timeout, self._flush_profile_buffer, args=[contact_id])
            self.profile_timers[contact_id] = timer
            timer.start()
            
    def _build_aggregated_profile_payload(self, event_type: str, buffer_data: Dict, contact_id: str) -> Dict:
        """Build complete profile payload from aggregated messages with ALL customer information"""
        # Extract all related table data
        cont_contacts = buffer_data.get('cont_contacts', {})
        cont_contact_profiles = buffer_data.get('cont_contact_profiles', {})
        cont_emails = buffer_data.get('cont_emails', {})
        cont_phone_numbers = buffer_data.get('cont_phone_numbers', {})
        cont_addresses = buffer_data.get('cont_addresses', {})
        cont_id_documents = buffer_data.get('cont_id_documents', {})
        
        main_data = cont_contacts.get('after', {}) if event_type == 'update-profile' else cont_contacts.get('before', {})
        
        if event_type == 'delete-profile':
            # Delete profile format
            return {
                "event": "delete-profile",
                "payload": {
                    "agent_id": main_data.get('last_modified_by'),
                    "contact_id": contact_id,
                    "last_modified_date": main_data.get('last_modified_date'),
                    "created_by": main_data.get('created_by'),
                    "last_modified_by": main_data.get('last_modified_by'),
                    "ts_ms": int(datetime.now().timestamp() * 1000)
                }
            }
        else:
            # Update profile format
            payload_data = {
                "contact_id": contact_id,
                "basic_info": cont_contacts.get('after', {}) if cont_contacts else {},
                "profile_details": cont_contact_profiles.get('after', {}) if cont_contact_profiles else {},
                "emails": cont_emails.get('after', {}) if cont_emails else {},
                "phone_numbers": cont_phone_numbers.get('after', {}) if cont_phone_numbers else {},
                "addresses": cont_addresses.get('after', {}) if cont_addresses else {},
                "id_documents": cont_id_documents.get('after', {}) if cont_id_documents else {},
                "event_type": "update-profile",
                "created_by": main_data.get('created_by'),
                "created_at": main_data.get('created_date'),
                "complete_profile": True
            }
            
            return {
                "event": "update-profile", 
                "payload": payload_data
            }
    
    def _flush_activity_buffer(self, task_id: str):
        """Flush aggregated activity messages and send to cleaned topic"""
        with self.buffer_lock:
            if task_id not in self.activity_message_buffer:
                return
                
            buffer_data = self.activity_message_buffer[task_id]
            del self.activity_message_buffer[task_id]
            
            # Cancel timer if exists
            if task_id in self.activity_timers:
                self.activity_timers[task_id].cancel()
                del self.activity_timers[task_id]
        
        # Build aggregated activity payload
        payload = self._build_aggregated_activity_payload(buffer_data, task_id)
        if payload:
            self._send_to_cleaned_topic("update-event", payload)
            logger.info(f"✅ Sent aggregated activity event for task_id {task_id}")
    
    def _add_to_activity_buffer(self, task_id: str, table_name: str, message_data: Dict):
        """Add message to activity buffer and set/reset timer"""
        with self.buffer_lock:
            self.activity_message_buffer[task_id][table_name] = message_data
            
            # Cancel existing timer
            if task_id in self.activity_timers:
                self.activity_timers[task_id].cancel()
            
            # Set new timer
            timer = threading.Timer(self.activity_timeout, self._flush_activity_buffer, args=[task_id])
            self.activity_timers[task_id] = timer
            timer.start()
            
    def _build_aggregated_activity_payload(self, buffer_data: Dict, task_id: str) -> Dict:
        """Build complete activity payload from aggregated messages"""
        # Extract all related table data
        relations_data = buffer_data.get('relations', {})
        tasks_data = buffer_data.get('tasks', {})
        
        # Build payload using 'after' data as requested
        ts_ms = int(datetime.now().timestamp() * 1000)
        
        # Start with relations data (primary)
        payload_data = {}
        if relations_data and relations_data.get('after'):
            payload_data = dict(relations_data.get('after', {}))
        
        # Add timestamp
        payload_data["ts_ms"] = ts_ms
        
        # Extract and add enrichment data
        if relations_data:
            rel_after = relations_data.get('after', {})
            
            # Add agent and contact info
            agent_id = rel_after.get('created_by')
            contact_id = rel_after.get('entity_id')
            
            if agent_id:
                payload_data["agent_id"] = str(agent_id)
            if contact_id:
                payload_data["contact_id"] = str(contact_id)
            if task_id:
                payload_data["task_id"] = str(task_id)
                
            # Add action and verified
            payload_data["action"] = "create"  # since we're using 'after' data
            payload_data["verified"] = True
        
        # Task enrichment from tasks table if available
        if tasks_data and tasks_data.get('after'):
            task_after = tasks_data.get('after', {})
            
            # Extract task_type_code and start_date
            task_type_code = task_after.get('task_type_code')
            if task_type_code:
                payload_data["task_type_code"] = str(task_type_code)
                
            start_date = task_after.get('start_date')
            if start_date:
                payload_data["start_date"] = str(start_date)
        
        return {
            "event": "update-event",
            "payload": payload_data
        }

    def _send_to_cleaned_topic(self, key: str, payload: Dict):
        """Send message to cleaned topic with proper headers"""
        try:
            kafka_headers = [('__TypeId__', b'net.vinid.core.event.EventMessage')]
            future = self.producer.send(
                self.cleaned_topic,
                key=key.encode('utf-8'),
                value=payload,
                headers=kafka_headers,
            )
            result = future.get(timeout=10)
            logger.info(f"✅ Message sent [{key}] → partition={result.partition}, offset={result.offset}")
            return True
        except Exception as send_err:
            logger.warning(f"⚠️ Send with headers failed, retrying without headers: {send_err}")
            try:
                future = self.producer.send(
                    self.cleaned_topic,
                    key=key.encode('utf-8'),
                    value=payload,
                )
                result = future.get(timeout=10)
                logger.info(f"✅ Message sent (no-headers) [{key}] → partition={result.partition}, offset={result.offset}")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to send message: {e}")
                return False

    def _should_send_profile_event(self, source_db: str, source_table: str, operation: str, before: Dict, after: Dict) -> str:
        """Detect profile events from ANY profile table"""
        if (source_db or '').lower() != 'omre_cbp_collab_qc':
            return None
            
        # Define all profile tables
        profile_tables = [
            'cont_contacts',           # Basic info
            'cont_contact_profiles',   # Detailed profile
            'cont_emails',             # Email addresses
            'cont_phone_numbers',      # Phone numbers
            'cont_addresses',          # Addresses
            'cont_id_documents'        # ID documents
        ]
        
        if source_table.lower() not in profile_tables:
            return None
            
        # CREATE profile: new contact/related data created
        if operation == 'c' and after:
            return 'update-profile'
            
        # UPDATE profile: any profile data changed
        if operation == 'u' and (before or after):
            # DELETE profile: only for cont_contacts status change ACTIVE -> DELETED
            if source_table.lower() == 'cont_contacts' and before and after:
                before_status = before.get('status', '').upper()
                after_status = after.get('status', '').upper()
                if before_status == 'ACTIVE' and after_status == 'DELETED':
                    return 'delete-profile'
            
            # 🎯 FILTER: Skip if ONLY last_interaction_date changed (triggered by activity events)
            if before and after:
                # Find actual meaningful changes (ignore timestamp-only updates)
                meaningful_changes = []
                ignore_fields = ['last_interaction_date', 'updated_at', 'last_modified_date', 'modified_date']
                
                # Check all fields for meaningful changes
                all_fields = set(before.keys()) | set(after.keys())
                for field in all_fields:
                    if field.lower() in [f.lower() for f in ignore_fields]:
                        continue  # Skip timestamp fields
                    
                    before_val = before.get(field)
                    after_val = after.get(field)
                    if before_val != after_val:
                        meaningful_changes.append(field)
                
                # If ONLY timestamp fields changed, skip this profile event
                if not meaningful_changes:
                    logger.info(f"↩️ Skipping profile update - only timestamp fields changed in {source_table}")
                    return None
            
            # Regular profile update for any table
            return 'update-profile'
                
        return None

    def _build_incremental_profile_payload(self, event_type: str, source_table: str, contact_id: str, 
                                          before_data: Dict, after_data: Dict, changed_columns: List[str], ts_ms: int) -> Dict:
        """Build profile payload in original format but only populate changed sections"""
        if event_type == 'delete-profile':
            # For delete, send minimal info (keep original delete format)
            return {
                "event": "delete-profile",
                "payload": {
                    "agent_id": before_data.get('last_modified_by') or after_data.get('last_modified_by'),
                    "contact_id": contact_id,
                    "last_modified_date": before_data.get('last_modified_date') or after_data.get('last_modified_date'),
                    "created_by": before_data.get('created_by') or after_data.get('created_by'),
                    "last_modified_by": before_data.get('last_modified_by') or after_data.get('last_modified_by'),
                    "ts_ms": ts_ms
                }
            }
        
        # For update-profile, use ORIGINAL format but only populate changed section
        payload_data = {
            "contact_id": contact_id,
            "basic_info": {},
            "profile_details": {},
            "emails": {},
            "phone_numbers": {},
            "addresses": {},
            "id_documents": {},
            "event_type": "update-profile",
            "created_by": after_data.get('created_by') if after_data else None,
            "created_at": after_data.get('created_date') if after_data else None,
            "complete_profile": False  # This is incremental update
        }
        
        # Populate ONLY the changed section with after_data
        if source_table == 'cont_contacts':
            payload_data["basic_info"] = after_data or {}
        elif source_table == 'cont_contact_profiles':
            payload_data["profile_details"] = after_data or {}
        elif source_table == 'cont_emails':
            payload_data["emails"] = after_data or {}
        elif source_table == 'cont_phone_numbers':
            payload_data["phone_numbers"] = after_data or {}
        elif source_table == 'cont_addresses':
            payload_data["addresses"] = after_data or {}
        elif source_table == 'cont_id_documents':
            payload_data["id_documents"] = after_data or {}
        
        return {
            "event": "update-profile",
            "payload": payload_data
        }

    def _build_profile_create_payload(self, after_mapped: Dict, actor: Dict, ts_ms: int) -> Dict:
        """Build comprehensive customer profile payload for CREATE events"""
        return {
            "op": "c",
            "source": {
                "db": "omre_cbp_collab_qc",
                "table": "cont_contacts",
                "ts_ms": ts_ms
            },
            "before": None,
            "after": after_mapped,
            "actor": actor,
            "profile_action": "CREATE",
            "customer_profile": {
                "basic_info": {
                    "contact_id": after_mapped.get("contact_id"),
                    "full_name": after_mapped.get("full_name"),
                    "type": after_mapped.get("type"),
                    "source": after_mapped.get("source"),
                    "channel": after_mapped.get("channel"),
                    "verification_status": after_mapped.get("verification_status"),
                    "created_by_agent": after_mapped.get("created_by"),
                    "created_timestamp": after_mapped.get("created_date"),
                    "note": after_mapped.get("note")
                }
            },
            "ts_ms": ts_ms
        }

    def _build_profile_delete_payload(self, before_mapped: Dict, after_mapped: Dict, changed_columns: List, actor: Dict, ts_ms: int) -> Dict:
        """Build minimal customer profile payload for DELETE events"""
        return {
            "op": "u",
            "source": {
                "db": "omre_cbp_collab_qc", 
                "table": "cont_contacts",
                "ts_ms": ts_ms
            },
            "changed_columns": changed_columns,
            "actor": actor,
            "profile_action": "DELETE",
            "deletion_info": {
                "contact_id": after_mapped.get("contact_id") if after_mapped else None,
                "full_name": after_mapped.get("full_name") if after_mapped else None,
                "deleted_by_agent": after_mapped.get("last_modified_by") if after_mapped else None,
                "deleted_timestamp": after_mapped.get("last_modified_date") if after_mapped else None,
                "previous_status": before_mapped.get("status") if before_mapped else None,
                "deletion_reason": "STATUS_CHANGE_TO_DELETED"
            },
            "ts_ms": ts_ms
        }
    
    def _is_meaningful_event(
        self,
        source_table: str,
        operation: str,
        actor: Dict[str, Any],
        contact_id: Any,
        agent_contact: Dict[str, Any],
    ) -> bool:
        """Decide if we have enough info to emit exactly one meaningful cleaned message.
        - relations: require agent_contact.agent_id and agent_contact.contact_id
        - others: require actor.omre_agent_id and (contact_id or agent_contact.contact_id)
        """
        table_lower = (source_table or '').lower()
        if table_lower == 'relations':
            return bool(agent_contact and agent_contact.get('agent_id') and agent_contact.get('contact_id'))
        has_actor = bool(actor and actor.get('omre_agent_id'))
        has_contact = bool(contact_id) or bool(agent_contact and agent_contact.get('contact_id'))
        return has_actor and has_contact

    def _heuristic_task_type_code(self, src: Dict[str, Any]) -> str:
        """When DB lookup is disabled and columns are UNKNOWN_COLx, try to find a value like 'email'/'call'."""
        if not isinstance(src, dict):
            return None
        # direct keys if present
        for k in ['task_type_code', 'task_type', 'type_code', 'channel', 'type']:
            if k in src and src[k] not in (None, ''):
                return str(src[k])
        # scan UNKNOWN_COLx values for known tokens
        candidates = []
        for k, v in src.items():
            if isinstance(k, str) and k.startswith('UNKNOWN_COL') and v not in (None, ''):
                s = str(v).strip().lower()
                if s in ('call', 'email'):
                    candidates.append(s)
        return candidates[0] if candidates else None
    
    def _extract_task_type_code(self,
        source_table: str,
        operation: str,
        before_mapped: Dict[str, Any],
        after_mapped: Dict[str, Any],
        before_raw: Dict[str, Any],
        after_raw: Dict[str, Any],
        columns: List[str],
    ) -> Optional[str]:
        try:
            src_for_type = after_mapped if operation != 'd' else before_mapped
            if isinstance(src_for_type, dict) and src_for_type.get('task_type_code') not in (None, ''):
                return str(src_for_type['task_type_code'])
            original = after_raw if operation != 'd' else before_raw
            if isinstance(original, dict):
                if isinstance(columns, list) and 'task_type_code' in columns:
                    try:
                        idx = columns.index('task_type_code')
                        key = f'UNKNOWN_COL{idx}'
                        if key in original and original[key] not in (None, ''):
                            return str(original[key])
                    except Exception:
                        pass
                if (source_table or '').lower() == 'tasks_unknown':
                    if 'UNKNOWN_COL9' in original and original['UNKNOWN_COL9'] not in (None, ''):
                        return str(original['UNKNOWN_COL9'])
        except Exception:
            return None
        return None
    
    def _extract_start_date(self,
        source_table: str,
        operation: str,
        before_mapped: Dict[str, Any],
        after_mapped: Dict[str, Any],
        before_raw: Dict[str, Any],
        after_raw: Dict[str, Any],
        columns: List[str],
    ) -> Optional[str]:
        """Extract start_date from the same raw CDC message.
        Priority:
        1) mapped field 'start_date' from after/before
        2) by ordinal index using schema cache (UNKNOWN_COL{idx}) for 'start_date'
        3) special-case tasks_unknown: use UNKNOWN_COL5
        """
        try:
            src = after_mapped if operation != 'd' else before_mapped
            if isinstance(src, dict) and src.get('start_date') not in (None, ''):
                return str(src['start_date'])
            original = after_raw if operation != 'd' else before_raw
            if isinstance(original, dict):
                if isinstance(columns, list) and 'start_date' in columns:
                    try:
                        idx = columns.index('start_date')
                        key = f'UNKNOWN_COL{idx}'
                        if key in original and original[key] not in (None, ''):
                            return str(original[key])
                    except Exception:
                        pass
                if (source_table or '').lower() == 'tasks_unknown':
                    if 'UNKNOWN_COL5' in original and original['UNKNOWN_COL5'] not in (None, ''):
                        return str(original['UNKNOWN_COL5'])
        except Exception:
            return None
        return None
    
    def _infer_activity_channel(self, source_table: str, before: Dict[str, Any], after: Dict[str, Any], operation: str) -> str:
        src = after if operation != 'd' else before
        if isinstance(src, dict):
            for k in ['channel', 'type', 'activity_type', 'action_type', 'communication_type', 'method']:
                if k in src and src[k]:
                    v = str(src[k]).lower()
                    if 'call' in v:
                        return 'call'
                    if 'email' in v:
                        return 'email'
        tbl = (source_table or '').lower()
        if 'call' in tbl or 'phone' in tbl:
            return 'call'
        if 'email' in tbl or 'mail' in tbl:
            return 'email'
        return None
    
    def process_message(self, message):
        """Process a single Kafka message"""
        try:
            # Parse Debezium message
            parsed = self.parse_debezium_message(message.value)
            if not parsed:
                return False
            source_db = parsed['source_db']
            source_table = parsed['source_table']
            operation = parsed['operation']
            
            # Only forward cleaned for omre_cbp_activity_qc
            if not self._should_send_to_cleaned(source_db):
                logger.info(f"↩️ Skipping cleaned send for {source_db}.{source_table} (filtered)")
                return False
            
            # Enrich: map UNKNOWN_COLx to real column names and include changed_columns
            payload = message.value.get('payload', {})
            before_raw = payload.get('before')
            after_raw = payload.get('after')
            # Preload schema for this table (from cache or DB if enabled) BEFORE using it
            columns = self._get_columns_for_table(source_db, source_table)
            before_mapped = self._map_unknown_columns(before_raw, columns) if before_raw is not None else before_raw
            after_mapped = self._map_unknown_columns(after_raw, columns) if after_raw is not None else after_raw
            changed_columns = []
            if operation == 'u':
                changed_columns = self._compute_changed_columns(before_mapped or {}, after_mapped or {})
            
            # Extract actor identifiers
            actor = self._extract_actor_ids(operation, source_table, before_mapped or {}, after_mapped or {})

            # Check for profile events (create/delete customer) - INSTANT send
            if (source_db or '').lower() == 'omre_cbp_collab_qc':
                # Define ONLY the profile tables we want to process
                profile_tables = [
                    'cont_contacts',           # Basic info
                    'cont_contact_profiles',   # Detailed profile (DOB, gender, job, assets, etc.)
                    'cont_emails',             # Email addresses
                    'cont_phone_numbers',      # Phone numbers
                    'cont_addresses',          # Addresses
                    'cont_id_documents'        # ID documents (CCCD/CMND)
                ]
                
                # ONLY process tables in the profile_tables list
                if source_table.lower() in profile_tables:
                    # Extract contact_id from the message
                    contact_id = None
                    if after_mapped and 'contact_id' in after_mapped:
                        contact_id = after_mapped['contact_id']
                    elif before_mapped and 'contact_id' in before_mapped:
                        contact_id = before_mapped['contact_id']
                    
                    if contact_id:
                        # INSTANT profile update - send only changed data
                        profile_event = self._should_send_profile_event(source_db, source_table, operation, before_mapped or {}, after_mapped or {})
                        if profile_event:
                            # Build incremental payload with only changed fields
                            payload = self._build_incremental_profile_payload(
                                profile_event, source_table, contact_id, 
                                before_mapped, after_mapped, changed_columns, 
                                parsed.get('timestamp', int(datetime.now().timestamp() * 1000))
                            )
                            if payload:
                                self._send_to_cleaned_topic(profile_event, payload)
                                logger.info(f"✅ Sent instant {profile_event} for {source_table} contact_id {contact_id}")
                        return True  # Message processed, don't process further
                    else:
                        logger.warning(f"⚠️ No contact_id found in {source_table} message")
                        return False
                else:
                    # Skip all other tables from collab database - only profile tables should be processed
                    logger.info(f"↩️ Skipping non-profile collab table: {source_table}")
                    return False

            # Check for activity events (relations/tasks) - use aggregation
            if (source_db or '').lower() == 'omre_cbp_activity_qc':
                # Check if this is an activity-related table
                activity_tables = ['relations', 'tasks']
                if source_table.lower() in activity_tables:
                    # Extract task_id from the message
                    task_id = None
                    if source_table.lower() == 'relations':
                        # For relations, task_id is in parent_id
                        if after_mapped and 'parent_id' in after_mapped:
                            task_id = after_mapped['parent_id']
                        elif before_mapped and 'parent_id' in before_mapped:
                            task_id = before_mapped['parent_id']
                    else:  # tasks table
                        # For tasks, task_id is in id field
                        if after_mapped and 'id' in after_mapped:
                            task_id = after_mapped['id']
                        elif before_mapped and 'id' in before_mapped:
                            task_id = before_mapped['id']
                        elif after_mapped and 'task_id' in after_mapped:
                            task_id = after_mapped['task_id']
                        elif before_mapped and 'task_id' in before_mapped:
                            task_id = before_mapped['task_id']
                    
                    if task_id:
                        # Add to aggregation buffer
                        message_data = {
                            'operation': operation,
                            'before': before_mapped,
                            'after': after_mapped,
                            'timestamp': parsed.get('timestamp', int(datetime.now().timestamp() * 1000))
                        }
                        self._add_to_activity_buffer(str(task_id), source_table.lower(), message_data)
                        logger.info(f"📝 Added {source_table} to activity buffer for task_id {task_id}")
                        return True  # Message buffered, don't process further
                    else:
                        logger.warning(f"⚠️ No task_id found in {source_table} message")
                        return False
            
            # Skip all other tables from activity database - only aggregated events should be sent
            if (source_db or '').lower() == 'omre_cbp_activity_qc':
                logger.info(f"↩️ Skipping non-aggregated activity table: {source_table}")
                return False

            # Extract contact info if contact_id present
            contact_obj = {}
            contact_id = None
            for src in (after_mapped or {}, before_mapped or {}):
                if 'contact_id' in src and src['contact_id'] not in (None, ''):
                    contact_id = src['contact_id']
                    break
            if contact_id is not None and self.enable_db_lookup:
                contact_obj = self._fetch_contact_info(contact_id)
            
            # Streaming correlation (no DB): remember relations by task_id and enrich tasks when seen
            try:
                now = time.time()
                # Garbage collect occasionally
                if len(self._relation_by_task) > 0 and (len(self._relation_by_task) % 100 == 0):
                    self._relation_by_task = {k: v for k, v in self._relation_by_task.items() if now - v.get('_ts', 0) < self._relation_ttl_seconds}
                table_lower = (source_table or '').lower()
                if table_lower == 'relations':
                    rel = self._extract_relation_link(operation, after_mapped or {}, before_mapped or {})
                    if 'task_id' in rel:
                        rel['_ts'] = now
                        self._relation_by_task[str(rel['task_id'])] = rel
                elif table_lower == 'tasks':
                    # if a relation already arrived, enrich contact_id and actor
                    task_id = None
                    for src in (after_mapped or {}, before_mapped or {}):
                        if 'task_id' in src and src['task_id'] not in (None, ''):
                            task_id = str(src['task_id']); break
                        if 'id' in src and src['id'] not in (None, ''):
                            task_id = str(src['id']); break
                    if task_id and task_id in self._relation_by_task:
                        rel = self._relation_by_task.get(task_id, {})
                        # backfill contact info from relation if missing
                        if not contact_id and 'contact_id' in rel:
                            contact_id = rel['contact_id']
                            if self.enable_db_lookup:
                                contact_obj = self._fetch_contact_info(contact_id)
                        # backfill actor from relation if missing
                        if (not actor or not actor.get('omre_agent_id')) and 'agent_id' in rel:
                            aid = rel['agent_id']
                            actor = {'omre_agent_id': str(aid), 'cbp_agent_id': str(aid)}
                    # cache task_type_code and start_date for later use by relations
                    if task_id:
                        ttc = self._extract_task_type_code(source_table, operation, before_mapped or {}, after_mapped or {}, before_raw or {}, after_raw or {}, columns)
                        sdt = self._extract_start_date(source_table, operation, before_mapped or {}, after_mapped or {}, before_raw or {}, after_raw or {}, columns)
                        if ttc or sdt:
                            cache_entry = {'_ts': now}
                            if ttc:
                                cache_entry['task_type_code'] = ttc
                            if sdt:
                                cache_entry['start_date'] = sdt
                            self._task_by_id[task_id] = cache_entry
            except Exception as _e:
                logger.debug(f"correlation skipped: {_e}")
            
            # Build agent-contact relationship
            if (source_table or '').lower() == 'relations':
                agent_contact = self._extract_relation_link(operation, after_mapped or {}, before_mapped or {})
            else:
                # stream-only: do not query DB for verification
                agent_contact = {}
                if contact_id or (actor and actor.get('omre_agent_id')):
                    agent_contact = {
                        **({'agent_id': actor.get('omre_agent_id')} if actor and actor.get('omre_agent_id') else {}),
                        **({'contact_id': str(contact_id)} if contact_id else {}),
                        'verified': False
                    }
            
            # Gate emission: only send when event is meaningful per rules
            if not self._is_meaningful_event(source_table, operation, actor, contact_id, agent_contact):
                logger.info(f"↩️ Skip emit (not meaningful) for {source_table} op={operation} actor={actor} contact_id={contact_id}")
                return False
            
            # Build passthrough message with clarified columns
            cleaned_message = dict(message.value)  # shallow copy original
            cleaned_payload = dict(payload)
            cleaned_payload['before'] = before_mapped
            cleaned_payload['after'] = after_mapped
            if changed_columns:
                cleaned_payload['changed_columns'] = changed_columns
            if actor:
                cleaned_payload['actor'] = actor
            if contact_obj:
                cleaned_payload['contact'] = contact_obj
            if agent_contact:
                cleaned_payload['agent_contact'] = agent_contact
            # infer activity channel (call/email) and include if available
            channel = self._infer_activity_channel(source_table, before_mapped or {}, after_mapped or {}, operation)
            if channel:
                cleaned_payload['activity_channel'] = channel
            # include task_type_code directly from raw/mapped rows (single message output)
            task_type_code = self._extract_task_type_code(
                source_table,
                operation,
                before_mapped or {},
                after_mapped or {},
                before_raw or {},
                after_raw or {},
                columns or [],
            )
            if task_type_code:
                cleaned_payload['task_type_code'] = task_type_code
                logger.info(f"   🏷 task_type_code: {task_type_code}")
            # include start_date directly from raw/mapped rows (single message output)
            start_date = self._extract_start_date(
                source_table,
                operation,
                before_mapped or {},
                after_mapped or {},
                before_raw or {},
                after_raw or {},
                columns or [],
            )
            if start_date:
                cleaned_payload['start_date'] = start_date
                logger.info(f"   📅 start_date: {start_date}")

            # For relations events, directly expose task_id at top-level payload by copying from parent_id
            if (source_table or '').lower() == 'relations':
                rel_src = after_mapped if operation != 'd' else before_mapped
                if isinstance(rel_src, dict):
                    rel_task_id = rel_src.get('parent_id') or rel_src.get('task_id')
                    if rel_task_id not in (None, ''):
                        cleaned_payload['task_id'] = str(rel_task_id)
                        # If we have cached task_type_code for this task_id from a prior tasks CDC, include it
                        if 'task_type_code' not in cleaned_payload or 'start_date' not in cleaned_payload:
                            t_cached = self._task_by_id.get(str(rel_task_id), {})
                            if isinstance(t_cached, dict):
                                if 'task_type_code' not in cleaned_payload and t_cached.get('task_type_code') not in (None, ''):
                                    cleaned_payload['task_type_code'] = str(t_cached['task_type_code'])
                                if 'start_date' not in cleaned_payload and t_cached.get('start_date') not in (None, ''):
                                    cleaned_payload['start_date'] = str(t_cached['start_date'])

            cleaned_message['payload'] = cleaned_payload
            
            # Kafka headers (outside payload)
            kafka_headers = [('__TypeId__', b'net.vinid.core.event.EventMessage')]
            
            # Key selection for activity events
            key_str = "update-event"
            try:
                future = self.producer.send(
                    self.cleaned_topic,
                    key=key_str.encode('utf-8'),
                    value=cleaned_message,
                    headers=kafka_headers,
                )
                result = future.get(timeout=10)
                logger.info(f"✅ Cleaned message sent [{source_db}.{source_table} {operation}] → partition={result.partition}, offset={result.offset}")
            except Exception as send_err:
                logger.warning(f"⚠️ Send with headers failed, retrying without headers: {send_err}", exc_info=True)
                future = self.producer.send(
                    self.cleaned_topic,
                    key=key_str.encode('utf-8'),
                    value=cleaned_message,
                )
                result = future.get(timeout=10)
                logger.info(f"✅ Cleaned message sent (no-headers) [{source_db}.{source_table} {operation}] → partition={result.partition}, offset={result.offset}")
            if changed_columns:
                logger.info(f"   🔎 Changed columns: {[c['column'] for c in changed_columns]}")
            if actor:
                logger.info(f"   🧑‍💼 Actor: {actor}")
            if contact_obj:
                logger.info(f"   👤 Contact: {contact_obj}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            return False
    
    def run(self):
        """Run the activity event pipeline"""
        logger.info("🎯 Activity Event Pipeline starting...")
        logger.info(f"📥 Reading from: {self.raw_topic}")
        logger.info(f"📤 Sending to: {self.cleaned_topic}")
        logger.info("💡 Processing only NEW messages (latest offset)")
        
        message_count = 0
        processed_count = 0
        
        try:
            for message in self.consumer:
                message_count += 1
                
                logger.info(f"📨 Processing message {message_count}:")
                logger.info(f"   Partition: {message.partition}, Offset: {message.offset}")
                
                if self.process_message(message):
                    processed_count += 1
                
                # Commit offset
                self.consumer.commit()
                
        except KeyboardInterrupt:
            logger.info("🛑 Pipeline stopped by user")
        except Exception as e:
            logger.error(f"❌ Pipeline error: {e}")
        finally:
            logger.info(f"📊 Pipeline summary:")
            logger.info(f"   Messages processed: {processed_count}/{message_count}")
            self.close()
    
    def close(self):
        """Close connections and cleanup timers"""
        # Cancel all pending timers
        with self.buffer_lock:
            # Profile timers
            for contact_id, timer in self.profile_timers.items():
                timer.cancel()
                logger.info(f"⏹️ Cancelled profile timer for contact_id {contact_id}")
            self.profile_timers.clear()
            
            # Activity timers  
            for task_id, timer in self.activity_timers.items():
                timer.cancel()
                logger.info(f"⏹️ Cancelled activity timer for task_id {task_id}")
            self.activity_timers.clear()
            
            # Flush remaining buffers
            for contact_id in list(self.profile_message_buffer.keys()):
                self._flush_profile_buffer(contact_id)
            for task_id in list(self.activity_message_buffer.keys()):
                self._flush_activity_buffer(task_id)
        
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        logger.info("🔌 Connections closed")

def main():
    """Main function"""
    pipeline = ActivityEventPipeline()
    
    if not pipeline.connect():
        print("❌ Failed to connect to Kafka")
        return
    
    print("""
🚀 Activity Event Pipeline Ready!

📋 CONFIGURATION:
✅ Raw topic: omre-cbp-cdp-raw-test-qc
✅ Cleaned topic: omre-cbp-cdp-cleaned-test-qc
✅ Processing mode: LATEST (only new messages)
✅ MySQL: mysql.qc.svc.cluster.local:3306 (Internal cluster DNS)
✅ Kafka: kafka.qc.svc.cluster.local:9092 (Internal cluster DNS)

🔎 Cleaned output now forwards ONLY omre_cbp_activity_qc.* messages.
   UNKNOWN_COLx are mapped to real column names and changed_columns are included.
    """)
    
    # Run the pipeline
    pipeline.run()

if __name__ == "__main__":
    main() 
