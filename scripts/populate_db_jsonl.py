import json
import psycopg2
from psycopg2.extras import execute_values
import os
import sys
from dotenv import load_dotenv

# --- CONFIGURATION ---
JSONL_FILE = "./scripts/TMDB/dataset1k.jsonl"
DATASET_NAME = "TMDb 1k"
BATCH_SIZE = 5000  # Rows per bulk insert

load_dotenv(".env")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )


# ─── Index / constraint management ──────────────────────────────────

# Only drop non-unique indexes for bulk speed. All unique constraints are kept
# active so that ON CONFLICT DO NOTHING correctly deduplicates during insert.
DROPPABLE_INDEXES = [
    # (index name, create DDL)
    ("idx_entities_collection",
     "CREATE INDEX idx_entities_collection ON entities(collection_global_id)"),
    ("idx_metadata_lookup",
     "CREATE INDEX idx_metadata_lookup ON metadata(key, value)"),
    ("idx_metadata_entity",
     "CREATE INDEX idx_metadata_entity ON metadata(entity_id)"),
    ("idx_ref_source_reason",
     "CREATE INDEX idx_ref_source_reason ON reference(source_id, reason)"),
    ("idx_ref_target_reason",
     "CREATE INDEX idx_ref_target_reason ON reference(target_id, reason)"),
]


def drop_indexes(cur):
    """Drop non-unique indexes for faster bulk inserts."""
    print("Dropping indexes ...")
    for name, _ in DROPPABLE_INDEXES:
        cur.execute(f"DROP INDEX IF EXISTS {name}")


def create_indexes(cur):
    """Recreate all indexes."""
    print("Recreating indexes ...")
    for name, ddl in DROPPABLE_INDEXES:
        print(f"  -> {name}")
        cur.execute(ddl)


# ─── Flush helpers ───────────────────────────────────────────────────

def flush_entities(cur, entity_buffer, entity_tracker, ent_map, col_map):
    """Bulk-insert a batch of entities and populate ent_map."""
    if not entity_buffer:
        return

    # Insert one-by-one to retrieve generated global_ids (execute_values
    # with RETURNING only gives last row with some drivers).
    # We use a single round-trip with unnest instead.
    cur.execute("""
        INSERT INTO entities (collection_global_id, original_id, name, contents)
        SELECT unnest(%(col_ids)s::int[]),
               unnest(%(orig_ids)s::int[]),
               unnest(%(names)s::varchar[]),
               unnest(%(contents)s::jsonb[])
        ON CONFLICT (collection_global_id, original_id) DO NOTHING
        RETURNING global_id, collection_global_id, original_id
    """, {
        "col_ids": [row[0] for row in entity_buffer],
        "orig_ids": [row[1] for row in entity_buffer],
        "names": [row[2] for row in entity_buffer],
        "contents": [row[3] for row in entity_buffer],
    })

    # Build a reverse map from (db_col_id, original_id) -> global_id
    db_col_to_original = {}
    for rows in cur.fetchall():
        gid, db_col_id, orig_id = rows
        db_col_to_original[(db_col_id, orig_id)] = gid

    # Now map back to (json_col_id, json_ent_id)
    # We need the reverse col_map for this
    rev_col_map = {v: k for k, v in col_map.items()}
    for (db_col_id, orig_id), gid in db_col_to_original.items():
        json_col_id = rev_col_map.get(db_col_id)
        if json_col_id is not None:
            ent_map[(json_col_id, orig_id)] = gid

    entity_buffer.clear()
    entity_tracker.clear()


def flush_metadata(cur, metadata_buffer):
    """Bulk-insert metadata rows."""
    if not metadata_buffer:
        return
    execute_values(
        cur,
        "INSERT INTO metadata (entity_id, key, value) VALUES %s ON CONFLICT DO NOTHING",
        metadata_buffer,
        page_size=BATCH_SIZE,
    )
    metadata_buffer.clear()


def flush_references(cur, reference_buffer):
    """Bulk-insert reference rows."""
    if not reference_buffer:
        return
    execute_values(
        cur,
        "INSERT INTO reference (source_id, target_id, reason) VALUES %s ON CONFLICT DO NOTHING",
        reference_buffer,
        page_size=BATCH_SIZE,
    )
    reference_buffer.clear()


# ─── Main population logic ───────────────────────────────────────────

def populate():
    conn = get_db_connection()

    try:
        cur = conn.cursor()

        # ── Read line 1: collections ──
        print(f"Streaming from {JSONL_FILE} ...")
        f = open(JSONL_FILE, "r", encoding="utf-8")

        header_line = f.readline()
        if not header_line:
            print("ERROR: JSONL file is empty.")
            return
        header = json.loads(header_line)
        collections = header["collections"]

        # ── Create dataset ──
        print(f"Creating Dataset: '{DATASET_NAME}' ...")
        cur.execute("""
            INSERT INTO datasets (name) VALUES (%s) 
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
        """, (DATASET_NAME,))
        dataset_id = cur.fetchone()[0]

        # ── Insert collections ──
        print("Inserting Collections ...")
        col_map = {}  # json_collection_id -> db_global_id
        for c in collections:
            cur.execute("""
                INSERT INTO collections (dataset_id, original_id, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (dataset_id, original_id) DO UPDATE SET name = EXCLUDED.name
                RETURNING global_id
            """, (dataset_id, c["id"], c["name"]))
            col_map[c["id"]] = cur.fetchone()[0]

        conn.commit()

        # ── Drop indexes for fast bulk insert ──
        drop_indexes(cur)
        conn.commit()

        # ── First pass: stream objects, insert entities ──
        print("Pass 1: Inserting entities ...")
        ent_map = {}          # (json_col_id, json_ent_id) -> db_global_id
        entity_buffer = []    # (db_col_id, original_id, name, contents_json)
        entity_tracker = []   # (json_col_id, json_ent_id) — parallel to entity_buffer
        obj_count = 0
        skipped = 0

        for line in f:
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)
            json_col_id = obj.get("collection_id")
            json_ent_id = obj.get("id")

            db_col_id = col_map.get(json_col_id)
            if db_col_id is None:
                skipped += 1
                continue

            contents = obj.get("contents", {})
            name = contents.get("Name", "Unknown")

            entity_buffer.append((db_col_id, json_ent_id, name, json.dumps(contents)))
            entity_tracker.append((json_col_id, json_ent_id))

            if len(entity_buffer) >= BATCH_SIZE:
                flush_entities(cur, entity_buffer, entity_tracker, ent_map, col_map)

            obj_count += 1
            if obj_count % 10000 == 0:
                print(f"\r  -> {obj_count:,} entities inserted", end="", flush=True)

        # Flush remaining
        flush_entities(cur, entity_buffer, entity_tracker, ent_map, col_map)
        print(f"\r  -> {obj_count:,} entities inserted (skipped {skipped})")
        conn.commit()

        # ── Second pass: stream objects again, insert metadata + references ──
        print("Pass 2: Inserting metadata and references ...")
        f.seek(0)
        f.readline()  # skip header

        metadata_buffer = []
        reference_buffer = []
        obj_count = 0

        for line in f:
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)
            source_key = (obj.get("collection_id"), obj.get("id"))
            source_global_id = ent_map.get(source_key)
            if source_global_id is None:
                continue

            # Metadata
            for key, val_list in obj.get("metadata", {}).items():
                for val in val_list:
                    metadata_buffer.append((source_global_id, key, val))

            # References
            for ref in obj.get("references", []):
                target_global_id = ent_map.get(
                    (ref["reference_collection_id"], ref["reference_id"])
                )
                if target_global_id is not None:
                    reference_buffer.append((source_global_id, target_global_id, ref["reason"]))

            if len(metadata_buffer) >= BATCH_SIZE:
                flush_metadata(cur, metadata_buffer)
            if len(reference_buffer) >= BATCH_SIZE:
                flush_references(cur, reference_buffer)

            obj_count += 1
            if obj_count % 10000 == 0:
                print(f"\r  -> {obj_count:,} objects processed", end="", flush=True)

        # Flush remaining
        flush_metadata(cur, metadata_buffer)
        flush_references(cur, reference_buffer)
        print(f"\r  -> {obj_count:,} objects processed")
        conn.commit()

        f.close()

        # ── Recreate indexes ──
        create_indexes(cur)
        conn.commit()

        print(f"Success! Imported data for dataset '{DATASET_NAME}'.")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    populate()
