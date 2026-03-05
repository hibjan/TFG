import os

try:
    import orjson

    def json_loads(s):
        return orjson.loads(s)

    def json_dumps(obj):
        return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")

    def json_dumps_pretty(obj):
        return orjson.dumps(
            obj, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS
        ).decode("utf-8")

except ImportError:
    import json

    def json_loads(s):
        return json.loads(s)

    def json_dumps(obj):
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    def json_dumps_pretty(obj):
        return json.dumps(obj, indent=4, ensure_ascii=False)


# ─── Global variables ────────────────────────────────────────────────
FORMAT_PATH = "scripts/TMDB/format.json"
OUTPUT_PATH = "scripts/TMDB/output.json"

# Toggle: True = range mode (numeric/date stored as ranges in metadata, raw in contents)
#         False = raw mode  (numeric/date stored as raw values in metadata only)
RANGE_MODE = True

# Toggle: True = compact JSON output (no indentation, ~40% smaller)
#         False = pretty-printed JSON output (indented, human-readable)
COMPACT_OUTPUT = False

# Write buffer size (8 MB)
WRITE_BUFFER_SIZE = 8 * 1024 * 1024

# Crew roles to include from credits.crew, keyed by department -> list of jobs
CREW_ROLES = {
    "Directing": ["Director"],
    "Production": ["Producer", "Executive Producer"],
    "Writing": ["Screenplay", "Writer"],
}


# ─── Helpers ─────────────────────────────────────────────────────────

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
            raw_objects.append(raw_obj)

    print(f"  -> {len(objects)} objects")
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

    # ── Movies ──
    if movies_cid is not None:
        for raw_obj in raw_by_collection.get(movies_cid, []):
            movie_id = raw_obj.get("id")
            movie = index.get((movies_cid, movie_id))
            if movie is None:
                continue

            # 1. production_companies -> Companies
            if companies_cid is not None:
                for company in raw_obj.get("production_companies", []) or []:
                    cid = company.get("id")
                    if cid is None:
                        continue
                    add_ref(movie, "Produced by", cid, companies_cid)
                    ref_count += 1
                    target = index.get((companies_cid, cid))
                    if target is not None:
                        add_ref(target, "Produced", movie_id, movies_cid)
                        ref_count += 1

            credits = raw_obj.get("credits", {}) or {}

            # 2. credits.cast -> People
            if people_cid is not None:
                for person in credits.get("cast", []) or []:
                    pid = person.get("id")
                    if pid is None:
                        continue
                    add_ref(movie, "Actor", pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, "Actor", movie_id, movies_cid)
                        ref_count += 1

            # 3. credits.crew (filtered by CREW_ROLES) -> People
            if people_cid is not None:
                for crew_member in credits.get("crew", []) or []:
                    dept = crew_member.get("department")
                    job = crew_member.get("job")
                    pid = crew_member.get("id")
                    if pid is None or dept not in CREW_ROLES:
                        continue
                    if job not in CREW_ROLES[dept]:
                        continue
                    add_ref(movie, job, pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, job, movie_id, movies_cid)
                        ref_count += 1

    # ── TV Series ──
    if tv_cid is not None:
        for raw_obj in raw_by_collection.get(tv_cid, []):
            tv_id = raw_obj.get("id")
            tv = index.get((tv_cid, tv_id))
            if tv is None:
                continue

            # 1. created_by -> People
            if people_cid is not None:
                for creator in raw_obj.get("created_by", []) or []:
                    pid = creator.get("id")
                    if pid is None:
                        continue
                    add_ref(tv, "Created by", pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, "Creator", tv_id, tv_cid)
                        ref_count += 1

            # 2. networks -> Networks
            if networks_cid is not None:
                for network in raw_obj.get("networks", []) or []:
                    nid = network.get("id")
                    if nid is None:
                        continue
                    add_ref(tv, "Broadcasted on", nid, networks_cid)
                    ref_count += 1
                    target = index.get((networks_cid, nid))
                    if target is not None:
                        add_ref(target, "Broadcasts", tv_id, tv_cid)
                        ref_count += 1

            # 3. production_companies -> Companies
            if companies_cid is not None:
                for company in raw_obj.get("production_companies", []) or []:
                    cid = company.get("id")
                    if cid is None:
                        continue
                    add_ref(tv, "Produced by", cid, companies_cid)
                    ref_count += 1
                    target = index.get((companies_cid, cid))
                    if target is not None:
                        add_ref(target, "Produced", tv_id, tv_cid)
                        ref_count += 1

            credits = raw_obj.get("credits", {}) or {}

            # 4. credits.cast -> People
            if people_cid is not None:
                for person in credits.get("cast", []) or []:
                    pid = person.get("id")
                    if pid is None:
                        continue
                    add_ref(tv, "Actor", pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, "Actor", tv_id, tv_cid)
                        ref_count += 1

            # 5. credits.crew (filtered by CREW_ROLES) -> People
            if people_cid is not None:
                for crew_member in credits.get("crew", []) or []:
                    dept = crew_member.get("department")
                    job = crew_member.get("job")
                    pid = crew_member.get("id")
                    if pid is None or dept not in CREW_ROLES:
                        continue
                    if job not in CREW_ROLES[dept]:
                        continue
                    add_ref(tv, job, pid, people_cid)
                    ref_count += 1
                    target = index.get((people_cid, pid))
                    if target is not None:
                        add_ref(target, job, tv_id, tv_cid)
                        ref_count += 1

    print(f"  -> {ref_count} references created")


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
        all_objects.extend(objs)
        raw_by_collection[coll_id] = raws

    # ── Second iteration: build references ──
    print("\nBuilding references ...")
    build_references(all_objects, raw_by_collection, col_id_by_name)

    # Free raw data after references are built
    del raw_by_collection

    # ── Third iteration: remove objects with no references AND no metadata ──
    before = len(all_objects)
    all_objects = [o for o in all_objects if o["references"] or o["metadata"]]
    pruned = before - len(all_objects)
    print(f"\nCleanup: removed {pruned} objects with no references and no metadata ({before} -> {len(all_objects)})")

    # ── Write output ──
    output = {
        "collections": out_collections,
        "objects": all_objects
    }

    serializer = json_dumps if COMPACT_OUTPUT else json_dumps_pretty
    with open(OUTPUT_PATH, "w", encoding="utf-8", buffering=WRITE_BUFFER_SIZE) as out:
        out.write(serializer(output))

    print(f"\nDone! Wrote {len(all_objects)} objects to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
