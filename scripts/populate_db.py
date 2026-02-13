import json
import psycopg2
from psycopg2.extras import execute_values
import os
import time

# --- CONFIGURATION ---
JSON_FILE = "./scripts/TMDB/tmdb_dataset.json"
DATASET_NAME = "TMDB"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "tfg_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )

def populate():
    conn = get_db_connection()

    try:
        cur = conn.cursor()
        
        print(f"Reading {JSON_FILE}...")
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # ---------------------------------------------------------
        # 1. CREATE DATASET
        # ---------------------------------------------------------
        print(f"Creating Dataset: '{DATASET_NAME}'...")
        cur.execute("""
            INSERT INTO datasets (name) VALUES (%s) 
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
        """, (DATASET_NAME,))
        dataset_id = cur.fetchone()[0]

        # ---------------------------------------------------------
        # 2. COLLECTIONS (Mapped to Dataset)
        # ---------------------------------------------------------
        print("Inserting Collections...")
        # Map: JSON Collection ID -> DB Global Collection ID
        col_map = {} 
        
        for c in data['collections']:
            cur.execute("""
                INSERT INTO collections (dataset_id, original_id, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (dataset_id, original_id) DO UPDATE SET name = EXCLUDED.name
                RETURNING global_id
            """, (dataset_id, c['id'], c['name']))
            
            global_col_id = cur.fetchone()[0]
            col_map[c['id']] = global_col_id

        # ---------------------------------------------------------
        # 3. ENTITIES (Pass 1)
        # ---------------------------------------------------------
        print("Inserting Entities (Pass 1)...")
        # Map: (JSON Col ID, JSON Ent ID) -> DB Global Ent ID
        ent_map = {} 
        entity_values = []
        entity_tracker = [] # Keep track of original IDs to build map

        for obj in data['objects']:
            json_col_id = obj['collection_id']
            json_ent_id = obj['id']
            
            # Lookup real collection ID
            db_col_id = col_map.get(json_col_id)
            if not db_col_id: continue

            contents = obj.get('contents', {})
            name = contents.get('name', 'Unknown')
            
            entity_values.append((db_col_id, json_ent_id, name, json.dumps(contents)))
            entity_tracker.append((json_col_id, json_ent_id))

        # Bulk Insert
        for i, val in enumerate(entity_values):
            cur.execute("""
                INSERT INTO entities (collection_global_id, original_id, name, contents)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (collection_global_id, original_id) DO NOTHING
                RETURNING global_id
            """, val)
            
            result = cur.fetchone()
            if result:
                new_global_id = result[0]
                json_col_id, json_ent_id = entity_tracker[i]
                ent_map[(json_col_id, json_ent_id)] = new_global_id

        # ---------------------------------------------------------
        # 4. METADATA & REFERENCES (Pass 2)
        # ---------------------------------------------------------
        print("Inserting Metadata and References (Pass 2)...")
        metadata_rows = []
        reference_rows = []

        for obj in data['objects']:
            source_global_id = ent_map.get((obj['collection_id'], obj['id']))
            if not source_global_id: continue

            # Metadata
            for key, val_list in obj.get('metadata', {}).items():
                for val in val_list:
                    metadata_rows.append((source_global_id, key, val))

            # References
            for ref in obj.get('references', []):
                target_global_id = ent_map.get((ref['reference_collection_id'], ref['reference_id']))
                if target_global_id:
                    reference_rows.append((source_global_id, target_global_id, ref['reason']))

        if metadata_rows:
            execute_values(cur, "INSERT INTO metadata (entity_id, key, value) VALUES %s ON CONFLICT DO NOTHING", metadata_rows)
        if reference_rows:
            execute_values(cur, "INSERT INTO reference (source_id, target_id, reason) VALUES %s ON CONFLICT DO NOTHING", reference_rows)

        conn.commit()
        print(f"Success! Imported data for dataset '{DATASET_NAME}'.")

    except Exception as e:
        print(f"Error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    populate()