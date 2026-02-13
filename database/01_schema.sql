-- Clean up
DROP TABLE IF EXISTS reference CASCADE;
DROP TABLE IF EXISTS metadata CASCADE;
DROP TABLE IF EXISTS entities CASCADE;
DROP TABLE IF EXISTS collections CASCADE;
DROP TABLE IF EXISTS datasets CASCADE;

-- 1. DATASETS (New Top Level)
CREATE TABLE datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL, -- e.g., "IMDB-Clone", "Arxiv-Papers"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. COLLECTIONS
-- Now scoped to a specific dataset.
-- We use 'global_id' as the PK, and store the JSON's 'original_id' for mapping.
CREATE TABLE collections (
    global_id SERIAL PRIMARY KEY,
    
    dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    original_id INTEGER NOT NULL, -- The ID from the JSON (e.g., 1, 2)
    name VARCHAR(255) NOT NULL,
    
    -- Ensure Collection IDs are unique *within* a dataset
    CONSTRAINT uq_collection_dataset UNIQUE (dataset_id, original_id)
);

-- 3. ENTITIES
-- References the specific collection's global_id.
CREATE TABLE entities (
    global_id SERIAL PRIMARY KEY,
    
    collection_global_id INTEGER NOT NULL REFERENCES collections(global_id) ON DELETE CASCADE,
    original_id INTEGER NOT NULL, -- The ID from the JSON
    
    name VARCHAR(512),
    contents JSONB,
    
    -- Unique within the specific collection
    CONSTRAINT uq_entity_collection UNIQUE (collection_global_id, original_id)
);

-- Indexes
CREATE INDEX idx_entities_collection ON entities(collection_global_id);
-- This allows the database to only look into relevant collections

-- 4. METADATA (Unchanged structure, relies on entity global_id)
CREATE TABLE metadata (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER NOT NULL REFERENCES entities(global_id) ON DELETE CASCADE,
    key VARCHAR(255) NOT NULL,
    value VARCHAR(512) NOT NULL,
    CONSTRAINT uq_metadata_entry UNIQUE (entity_id, key, value)
);

CREATE INDEX idx_metadata_lookup ON metadata(key, value);
-- Ordering by key and value create a B-Tree that makes it fast to look up (useful for add_mfilter)
CREATE INDEX idx_metadata_entity ON metadata(entity_id);
-- Makes it fast to find metadata for a specific entity (useful for Details View and Dynamic Facets)

-- 5. REFERENCES (Unchanged structure, relies on entity global_id)
CREATE TABLE reference (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES entities(global_id) ON DELETE CASCADE,
    target_id INTEGER NOT NULL REFERENCES entities(global_id) ON DELETE CASCADE,
    reason VARCHAR(255) NOT NULL,
    CONSTRAINT uq_reference UNIQUE (source_id, target_id, reason)
);

CREATE INDEX idx_ref_source_reason ON reference(source_id, reason);
-- It allows to find links (link). Usually we want highest cardinality first.
-- But to make a role filter it might be needed to add (reason, source_id)
CREATE INDEX idx_ref_target_reason ON reference(target_id, reason);
-- With this it's easily available to find the result (add_rfilter)