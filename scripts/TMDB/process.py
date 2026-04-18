import os
import orjson

# ─── Global variables ────────────────────────────────────────────────
FORMAT_PATH = "scripts/TMDB/format.json"
OUTPUT_PATH = "scripts/TMDB/dataset_top500.jsonl"

# Toggle: True = filter Movies and TV Series using Bayesian average of ratings
FILTER_BY_BAYESIAN_AVERAGE = True
# Minimum number of votes required for Bayesian average calculation
MIN_VOTES_THRESHOLD = 100
# Minimum Bayesian average score to include an item (0-10 scale)
MIN_BAYESIAN_SCORE = 5.0
# Global minimum rating used in Bayesian average formula (typically the platform's average rating)
GLOBAL_MIN_RATING = 5.0
# Maximum number of top-rated Movies and TV Series to keep (applied per collection)
TOP_K = 500

# Toggle: True = range mode (numeric/date stored as ranges in metadata, raw in contents)
#         False = raw mode  (numeric/date stored as raw values in metadata only)
RANGE_MODE = True

# Toggle: True = compact JSON output (no indentation, ~40% smaller)
#         False = pretty-printed JSON output (indented, human-readable)
COMPACT_OUTPUT = True

# Write buffer size (8 MB)
WRITE_BUFFER_SIZE = 8 * 1024 * 1024

# Crew roles to include from credits.crew, keyed by department -> list of jobs
CREW_ROLES = {
    "Directing": ["Director"],
    "Production": ["Producer", "Executive Producer"],
    "Writing": ["Screenplay", "Novel"],
    "Sound": ["Original Music Composer"],
    "Photography": ["Director of Photography"],
}


# ─── Helpers ─────────────────────────────────────────────────────────

def json_loads(s):
        return orjson.loads(s)

def json_dumps(obj):
    return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")

def json_dumps_pretty(obj):
    return orjson.dumps(
        obj, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS
    ).decode("utf-8")

def is_empty(value):
    """Return True if a value should be considered empty (None, 0, or empty string)."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)) and value == 0:
        return True
    return False


def format_number(n):
    """Format a number with human-readable suffixes: K, M, B.
    e.g. 49999999 -> '49.999M', 1000 -> '1K', 999 -> '999'.
    Uses integer arithmetic to avoid float rounding issues.
    """
    n = int(n)
    negative = n < 0
    n = abs(n)

    if n >= 1_000_000_000:
        whole, frac = divmod(n, 1_000_000_000)
        suffix = "B"
        divisor = 1_000_000_000
    elif n >= 1_000_000:
        whole, frac = divmod(n, 1_000_000)
        suffix = "M"
        divisor = 1_000_000
    elif n >= 1_000:
        whole, frac = divmod(n, 1_000)
        suffix = "K"
        divisor = 1_000
    else:
        s = str(n)
        return f"-{s}" if negative else s

    if frac == 0:
        s = f"{whole}{suffix}"
    else:
        # Build decimal part from the remainder, stripping trailing zeros
        frac_str = str(frac).zfill(len(str(divisor)) - 1).rstrip("0")[:3]
        s = f"{whole}.{frac_str}{suffix}"

    return f"-{s}" if negative else s


def bucket_range(value, bucket_size):
    """Return a human-readable range string for a numeric value using fixed-width buckets.
    e.g. with bucket_size=50000000: '0-49.999M', '50M-99.999M', etc.
    """
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None

    if num < 0:
        bucket_start = -bucket_size * (int(abs(num) // bucket_size) + 1)
        return f"{format_number(bucket_start)}-{format_number(bucket_start + bucket_size - 1)}"

    bucket_start = int(num // bucket_size) * bucket_size
    return f"{format_number(bucket_start)}-{format_number(bucket_start + bucket_size - 1)}"


def calculate_bayesian_average(vote_count, vote_average):
    """Calculate Bayesian average rating.
    Formula: (vote_count * vote_average + MIN_VOTES_THRESHOLD * GLOBAL_MIN_RATING) / (vote_count + MIN_VOTES_THRESHOLD)
    This gives more weight to items with more votes while preventing low-vote items from dominating.
    """
    try:
        v_count = float(vote_count) if vote_count is not None else 0
        v_avg = float(vote_average) if vote_average is not None else 0
        if v_count < 0 or v_avg < 0 or v_avg > 10:
            return 0.0
        return (v_count * v_avg + MIN_VOTES_THRESHOLD * GLOBAL_MIN_RATING) / (v_count + MIN_VOTES_THRESHOLD)
    except (TypeError, ValueError):
        return 0.0


def precompute_code_dicts(metadata_fields):
    """Pre-convert coded field code lists from list-of-dicts to a single dict for O(1) lookups."""
    for field_cfg in metadata_fields.values():
        if field_cfg.get("type") == "coded" and "code" in field_cfg:
            code_dict = {}
            for entry in field_cfg["code"]:
                code_dict.update(entry)
            field_cfg["_code_dict"] = code_dict


def extract_field_value(raw_obj, field_name, is_multiple, multiple_field):
    """Extract a raw value (or list of values) from a JSONL object.
    Returns a list of raw values.
    """
    raw_value = raw_obj.get(field_name)

    if is_empty(raw_value):
        return []

    if is_multiple:
        if not isinstance(raw_value, list):
            return [] if is_empty(raw_value) else [str(raw_value)]

        if multiple_field:
            return [str(item[multiple_field]) for item in raw_value if multiple_field in item and not is_empty(item[multiple_field])]
        else:
            return [str(v) for v in raw_value if not is_empty(v)]
    else:
        return [raw_value]


def process_metadata_field(meta_name, field_type, field_cfg, raw_values):
    """Process raw values into metadata entries and optional extra contents.
    Returns (metadata_dict_additions, contents_dict_additions).
    """
    metadata = {}
    contents = {}

    if field_type == "string":
        str_values = [str(v) for v in raw_values]
        if str_values:
            metadata[meta_name] = str_values

    elif field_type == "coded":
        code_dict = field_cfg.get("_code_dict", {})
        decoded = [code_dict.get(str(v), str(v)) for v in raw_values]
        if decoded:
            metadata[meta_name] = decoded

    elif field_type == "numeric":
        range_size = field_cfg.get("range", 1000)
        if RANGE_MODE:
            ranges = []
            for v in raw_values:
                r = bucket_range(v, range_size)
                if r is not None:
                    ranges.append(r)
                contents[f"{meta_name} (raw)"] = str(v)
            if ranges:
                metadata[meta_name] = ranges
        else:
            str_values = [str(v) for v in raw_values]
            if str_values:
                metadata[meta_name] = str_values

    elif field_type == "date":
        range_year = field_cfg.get("range_year", 10)
        range_month = field_cfg.get("range_month", 1)
        range_day = field_cfg.get("range_day", 1)
        for v in raw_values:
            if v is None:
                continue
            date_str = str(v)
            parts = date_str.split("-")
            year = parts[0] if len(parts) >= 1 else None
            month = parts[1] if len(parts) >= 2 else None
            day = parts[2] if len(parts) >= 3 else None

            if year is None:
                continue

            if RANGE_MODE:
                contents[f"{meta_name} (raw)"] = date_str

                y = int(year)
                y_start = (y // range_year) * range_year
                y_end = y_start + range_year - 1
                metadata.setdefault(f"{meta_name} (Year)", []).append(f"{y_start}-{y_end}")

                if month is not None:
                    m = int(month)
                    m_start = ((m - 1) // range_month) * range_month + 1
                    m_end = min(m_start + range_month - 1, 12)
                    metadata.setdefault(f"{meta_name} (Month)", []).append(
                        str(m_start) if m_start == m_end else f"{m_start}-{m_end}")
                if day is not None:
                    d = int(day)
                    d_start = ((d - 1) // range_day) * range_day + 1
                    d_end = min(d_start + range_day - 1, 31)
                    metadata.setdefault(f"{meta_name} (Day)", []).append(
                        str(d_start) if d_start == d_end else f"{d_start}-{d_end}")
            else:
                metadata.setdefault(f"{meta_name} (Year)", []).append(year)
                if month is not None:
                    metadata.setdefault(f"{meta_name} (Month)", []).append(month)
                if day is not None:
                    metadata.setdefault(f"{meta_name} (Day)", []).append(day)

    return metadata, contents


def extract_references_data(raw_obj):
    """Extract only the fields needed for the second iteration to save memory."""
    refs = {"id": raw_obj.get("id")}
    
    comp = [c.get("id") for c in raw_obj.get("production_companies") or [] if c.get("id") is not None]
    if comp: refs["companies"] = comp

    creat = [c.get("id") for c in raw_obj.get("created_by") or [] if c.get("id") is not None]
    if creat: refs["creators"] = creat
    
    net = [n.get("id") for n in raw_obj.get("networks") or [] if n.get("id") is not None]
    if net: refs["networks"] = net
    
    credits = raw_obj.get("credits") or {}
    cast = [c.get("id") for c in credits.get("cast") or [] if c.get("id") is not None]
    if cast: refs["cast"] = cast
    
    crew = []
    for c in credits.get("crew") or []:
        dept = c.get("department")
        job = c.get("job")
        cid = c.get("id")
        if cid is not None and dept in CREW_ROLES and job in CREW_ROLES[dept]:
            crew.append((cid, job))
    if crew: refs["crew"] = crew
    
    return refs if len(refs) > 1 else None


def process_collection(collection_cfg, object_cfg):
    """Process all objects in a collection's JSONL file.
    Returns (list_of_output_objects, list_of_raw_objects).
    Raw objects are kept for reference extraction in the second iteration.
    """
    input_file = collection_cfg["input_file"]
    collection_id = collection_cfg["id"]
    metadata_fields = object_cfg.get("metadata", {})
    contents_fields = object_cfg.get("contents", {})
    resources_fields = object_cfg.get("resources", {})

    # Pre-convert coded field lookups for this collection
    precompute_code_dicts(metadata_fields)

    # Pre-extract field configs to avoid repeated .get() calls per object
    meta_configs = []
    for meta_name, field_cfg in metadata_fields.items():
        meta_configs.append((
            meta_name,
            field_cfg["name"],
            field_cfg["type"],
            field_cfg.get("multiple", False),
            field_cfg.get("multiple_field"),
            field_cfg,
        ))

    content_configs = []
    for display_name, content_cfg in contents_fields.items():
        content_configs.append((display_name, content_cfg["name"]))

    resource_configs = []
    for label, res_cfg in resources_fields.items():
        resource_configs.append((
            label,
            res_cfg["name"],
            res_cfg["type"],
            res_cfg.get("base_url", ""),
        ))

    objects = []
    raw_objects = []
    seen_ids = set()  # Track (collection_id, id) to detect duplicates

    if not os.path.exists(input_file):
        print(f"WARNING: Input file not found: {input_file}")
        return objects, raw_objects

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                raw_obj = json_loads(line)
            except (ValueError, Exception):
                continue

            obj_id = raw_obj.get("id")
            if obj_id is None:
                continue

            # Duplicate detection
            key = (collection_id, obj_id)
            if key in seen_ids:
                print(f"  WARNING: Duplicate (collection_id={collection_id}, id={obj_id}), skipping")
                continue
            seen_ids.add(key)

            # ── Build contents ──
            obj_contents = {}
            for display_name, raw_field_name in content_configs:
                value = raw_obj.get(raw_field_name)
                if not is_empty(value):
                    obj_contents[display_name] = value if isinstance(value, str) else str(value)

            # ── Build metadata ──
            obj_metadata = {}
            for meta_name, field_name, field_type, is_multiple, multiple_field, field_cfg in meta_configs:
                raw_values = extract_field_value(raw_obj, field_name, is_multiple, multiple_field)
                if not raw_values:
                    continue
                meta_additions, contents_additions = process_metadata_field(
                    meta_name, field_type, field_cfg, raw_values
                )
                obj_metadata.update(meta_additions)
                obj_contents.update(contents_additions)

            # ── Build resources ──
            obj_resources = []
            for label, field_name, res_type, base_url in resource_configs:
                raw_value = raw_obj.get(field_name)
                if is_empty(raw_value):
                    continue
                raw_value = str(raw_value).strip()
                if not raw_value:
                    continue

                if res_type == "image":
                    url = f"{base_url}{raw_value}" if base_url else raw_value
                    obj_resources.append({"type": "image", "label": label, "url": url})
                elif res_type == "link":
                    obj_resources.append({"type": "link", "label": label, "url": raw_value})
                elif res_type == "imdb":
                    if raw_value.startswith("nm"):
                        imdb_url = f"https://www.imdb.com/name/{raw_value}"
                    else:
                        imdb_url = f"https://www.imdb.com/title/{raw_value}"
                    obj_resources.append({"type": "link", "label": label, "url": imdb_url})

            if obj_resources:
                obj_contents["_resources"] = obj_resources

            out_obj = {
                "id": obj_id,
                "collection_id": collection_id,
                "metadata": obj_metadata,
                "references": [],
                "contents": obj_contents
            }
            objects.append(out_obj)
            
            ref_data = extract_references_data(raw_obj)
            if ref_data is not None:
                raw_objects.append(ref_data)

            if len(objects) % 10000 == 0:
                print(f"\r  -> {len(objects):,} objects created", end="", flush=True)

    print(f"\r  -> {len(objects):,} objects created")
    return objects, raw_objects


def add_ref(obj, reason, ref_id, ref_collection_id):
    """Add a reference to an object, avoiding duplicates."""
    ref = {"reason": reason, "reference_id": ref_id, "reference_collection_id": ref_collection_id}
    obj["references"].append(ref)


def build_references(all_objects, raw_by_collection, col_id_by_name):
    """Second iteration: build bidirectional references between objects.
    Uses an inverted index (collection_id, obj_id) -> obj for O(1) lookups.
    """
    # Build inverted index: (collection_id, obj_id) -> output object
    index = {}
    for obj in all_objects:
        index[(obj["collection_id"], obj["id"])] = obj

    movies_cid = col_id_by_name.get("Movies")
    tv_cid = col_id_by_name.get("TV Series")
    people_cid = col_id_by_name.get("People")
    companies_cid = col_id_by_name.get("Companies")
    networks_cid = col_id_by_name.get("Networks")

    ref_count = 0
    last_printed_refs = 0

    # ── Movies ──
    if movies_cid is not None:
        for ref_data in raw_by_collection.get(movies_cid, []):
            movie_id = ref_data["id"]
            movie = index.get((movies_cid, movie_id))
            if movie is None:
                continue

            # 1. production_companies -> Companies
            if companies_cid is not None:
                for cid in ref_data.get("companies", []):
                    add_ref(movie, "Produced by", cid, companies_cid)
                    ref_count += 1
                    target = index.get((companies_cid, cid))
                    if target is not None:
                        add_ref(target, "Produced", movie_id, movies_cid)
                        ref_count += 1

            # 2. credits.cast -> People
            if people_cid is not None:
                for pid in ref_data.get("cast", []):
                    add_ref(movie, "Actor", pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, "Actor", movie_id, movies_cid)
                        ref_count += 1

            # 3. credits.crew -> People
            if people_cid is not None:
                for pid, job in ref_data.get("crew", []):
                    add_ref(movie, job, pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, job, movie_id, movies_cid)
                        ref_count += 1

            if ref_count - last_printed_refs >= 10000:
                print(f"\r  -> {ref_count:,} references created", end="", flush=True)
                last_printed_refs = ref_count

    # ── TV Series ──
    if tv_cid is not None:
        for ref_data in raw_by_collection.get(tv_cid, []):
            tv_id = ref_data["id"]
            tv = index.get((tv_cid, tv_id))
            if tv is None:
                continue

            # 1. created_by -> People
            if people_cid is not None:
                for pid in ref_data.get("creators", []):
                    add_ref(tv, "Created by", pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, "Creator", tv_id, tv_cid)
                        ref_count += 1

            # 2. networks -> Networks
            if networks_cid is not None:
                for nid in ref_data.get("networks", []):
                    add_ref(tv, "Broadcasted on", nid, networks_cid)
                    ref_count += 1
                    target = index.get((networks_cid, nid))
                    if target is not None:
                        add_ref(target, "Broadcasts", tv_id, tv_cid)
                        ref_count += 1

            # 3. production_companies -> Companies
            if companies_cid is not None:
                for cid in ref_data.get("companies", []):
                    add_ref(tv, "Produced by", cid, companies_cid)
                    ref_count += 1
                    target = index.get((companies_cid, cid))
                    if target is not None:
                        add_ref(target, "Produced", tv_id, tv_cid)
                        ref_count += 1

            # 4. credits.cast -> People
            if people_cid is not None:
                for pid in ref_data.get("cast", []):
                    add_ref(tv, "Actor", pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, "Actor", tv_id, tv_cid)
                        ref_count += 1

            # 5. credits.crew -> People
            if people_cid is not None:
                for pid, job in ref_data.get("crew", []):
                    add_ref(tv, job, pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, job, tv_id, tv_cid)
                        ref_count += 1

            if ref_count - last_printed_refs >= 10000:
                print(f"\r  -> {ref_count:,} references created", end="", flush=True)
                last_printed_refs = ref_count

    print(f"\r  -> {ref_count:,} references created")


# ─── Main ────────────────────────────────────────────────────────────

def main():
    # Load format configuration
    with open(FORMAT_PATH, "r", encoding="utf-8") as f:
        fmt = json_loads(f.read())

    collections = fmt["collections"]
    object_cfgs = fmt["objects"]

    # Build lookups
    obj_cfg_map = {cfg["collection_id"]: cfg for cfg in object_cfgs}
    col_id_by_name = {c["name"]: c["id"] for c in collections}

    # Output collections (strip input_file, keep name + id only)
    out_collections = [{"name": c["name"], "id": c["id"]} for c in collections]

    # ── First iteration: build all objects ──
    all_objects = []
    raw_by_collection = {}  # collection_id -> list of raw JSONL dicts

    for coll in collections:
        coll_id = coll["id"]
        obj_cfg = obj_cfg_map.get(coll_id)
        if obj_cfg is None:
            print(f"WARNING: No object config for collection {coll['name']} (id={coll_id})")
            continue

        print(f"Processing collection: {coll['name']} ...")
        objs, raws = process_collection(coll, obj_cfg)

        if FILTER_BY_BAYESIAN_AVERAGE and coll["name"] in ["Movies", "TV Series"]:
            print(f"  Filtering {coll['name']} by Bayesian average (min_votes={MIN_VOTES_THRESHOLD}, min_score={MIN_BAYESIAN_SCORE})...")
            
            def get_bayesian_score(obj):
                metadata = obj.get("metadata", {})
                # Extract vote_count and vote_average from metadata
                # Assuming these are stored as lists in metadata (take first value)
                vote_count_list = metadata.get("Vote Count", [])
                vote_avg_list = metadata.get("Vote Average", [])
                
                vote_count = vote_count_list[0] if vote_count_list else 0
                vote_avg = vote_avg_list[0] if vote_avg_list else 0
                
                return calculate_bayesian_average(vote_count, vote_avg)
            
            # Calculate Bayesian score for each object
            scored_objs = [(obj, get_bayesian_score(obj)) for obj in objs]
            # Filter by minimum score threshold
            scored_objs = [(obj, score) for obj, score in scored_objs if score >= MIN_BAYESIAN_SCORE]
            # Sort by Bayesian score (descending)
            scored_objs.sort(key=lambda x: x[1], reverse=True)
            # Keep only top K items
            scored_objs = scored_objs[:TOP_K]
            # Extract filtered objects
            objs = [obj for obj, score in scored_objs]
            
            print(f"    -> Kept {len(objs)} {coll['name']} with Bayesian score >= {MIN_BAYESIAN_SCORE} (top {TOP_K})")
            
            # Keep only the raw reference tracker objects for the filtered set
            kept_ids = {obj["id"] for obj in objs}
            raws = [r for r in raws if r["id"] in kept_ids]

        all_objects.extend(objs)
        raw_by_collection[coll_id] = raws

    # ── Second iteration: build references ──
    print("\nBuilding references ...")
    build_references(all_objects, raw_by_collection, col_id_by_name)

    # Free raw data after references are built
    del raw_by_collection

    # ── Third iteration: remove objects based on settings ──
    before = len(all_objects)
    if FILTER_BY_BAYESIAN_AVERAGE:
        all_objects = [o for o in all_objects if o["references"] and o["metadata"]]
    else:
        all_objects = [o for o in all_objects if o["references"] or o["metadata"]]
    
    pruned = before - len(all_objects)
    print(f"\nCleanup: removed {pruned} objects ({before} -> {len(all_objects)})")

    # ── Write output ──
    print(f"\nWriting output to {OUTPUT_PATH} ...")
    
    with open(OUTPUT_PATH, "w", encoding="utf-8", buffering=WRITE_BUFFER_SIZE) as out:
        if COMPACT_OUTPUT:
            out.write(json_dumps({"collections": out_collections}) + '\n')
            
            for i, obj in enumerate(all_objects):
                out.write(json_dumps(obj) + '\n')
                if i % 10000 == 0:
                    print(f"\r  -> Wrote {i:,} / {len(all_objects):,} objects", end="", flush=True)
        else:
            out.write('{\n  "collections": ')
            out.write(json_dumps_pretty(out_collections))
            out.write(',\n  "objects": [\n')
            
            for i, obj in enumerate(all_objects):
                if i > 0:
                    out.write(',\n')
                # Indent internal object string by 4 spaces
                obj_str = json_dumps_pretty(obj)
                indented_obj = '\n'.join('    ' + line if line else line for line in obj_str.split('\n'))
                out.write(indented_obj)
                
                if i % 10000 == 0:
                    print(f"\r  -> Wrote {i:,} / {len(all_objects):,} objects", end="", flush=True)
                    
            out.write('\n  ]\n}')

    print(f"\r  -> Wrote {len(all_objects):,} / {len(all_objects):,} objects")
    print(f"\nDone! Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
