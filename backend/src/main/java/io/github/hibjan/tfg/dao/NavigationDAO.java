package io.github.hibjan.tfg.dao;

import io.github.hibjan.tfg.model.Link;
import io.github.hibjan.tfg.model.State;

import java.sql.*;
import java.util.*;

public class NavigationDAO {

    /**
     * Get available datasets.
     */
    public List<Map<String, Object>> getDatasets() throws SQLException {
        List<Map<String, Object>> datasets = new ArrayList<>();

        String sql = "SELECT id, name FROM datasets ORDER BY name";

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                Map<String, Object> ds = new HashMap<>();
                ds.put("id", rs.getInt("id"));
                ds.put("name", rs.getString("name"));
                datasets.add(ds);
            }
        }
        return datasets;
    }

    /**
     * Get collections for a dataset.
     */
    public List<Map<String, Object>> getCollections(int datasetId) throws SQLException {
        List<Map<String, Object>> collections = new ArrayList<>();

        String sql = "SELECT original_id, name FROM collections WHERE dataset_id = ? ORDER BY name";

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, datasetId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> col = new HashMap<>();
                    col.put("id", rs.getInt("original_id"));
                    col.put("name", rs.getString("name"));
                    collections.add(col);
                }
            }
        }
        return collections;
    }

    /**
     * Get available entities based on current state filters.
     */
    public List<Map<String, Object>> getAvailableEntities(State state, int page, int size)
            throws SQLException {

        List<Map<String, Object>> results = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Base query: entities in current collection
        sql.append("""
                SELECT DISTINCT e.original_id, e.name
                FROM entities e
                JOIN collections c ON e.collection_global_id = c.global_id
                WHERE c.dataset_id = ? AND c.original_id = ?
                """);
        params.add(state.getDatasetId());
        params.add(state.getCurrentCollectionId());

        appendFilterClauses(sql, params, state);

        sql.append(" ORDER BY e.name LIMIT ? OFFSET ?");
        params.add(size);
        params.add(page * size);

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> entity = new HashMap<>();
                    entity.put("id", rs.getInt("original_id"));
                    entity.put("name", rs.getString("name"));
                    results.add(entity);
                }
            }
        }
        return results;
    }

    /**
     * Get entities matching ANY of the states in the unionSet.
     */
    public List<Map<String, Object>> getUnionEntities(List<State> unionSet, int page, int size)
            throws SQLException {
        if (unionSet == null || unionSet.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> results = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();

        sql.append("""
                SELECT DISTINCT e.original_id, e.name, c.original_id as collection_id
                FROM entities e
                JOIN collections c ON e.collection_global_id = c.global_id
                WHERE
                """);

        for (int i = 0; i < unionSet.size(); i++) {
            State state = unionSet.get(i);
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append(" (c.dataset_id = ? AND c.original_id = ? ");
            params.add(state.getDatasetId());
            params.add(state.getCurrentCollectionId());

            appendFilterClauses(sql, params, state);

            sql.append(" ) ");
        }

        sql.append(" ORDER BY e.name LIMIT ? OFFSET ?");
        params.add(size);
        params.add(page * size);

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> entity = new HashMap<>();
                    entity.put("id", rs.getInt("original_id"));
                    entity.put("name", rs.getString("name"));
                    entity.put("collection_id", rs.getInt("collection_id"));
                    results.add(entity);
                }
            }
        }
        return results;
    }

    /**
     * Count total unique entities matching ANY of the states in the unionSet.
     */
    public int countUnionEntities(List<State> unionSet) throws SQLException {
        if (unionSet == null || unionSet.isEmpty()) {
            return 0;
        }

        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();

        sql.append("""
                SELECT COUNT(DISTINCT e.global_id)
                FROM entities e
                JOIN collections c ON e.collection_global_id = c.global_id
                WHERE
                """);

        for (int i = 0; i < unionSet.size(); i++) {
            State state = unionSet.get(i);
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append(" (c.dataset_id = ? AND c.original_id = ? ");
            params.add(state.getDatasetId());
            params.add(state.getCurrentCollectionId());

            appendFilterClauses(sql, params, state);

            sql.append(" ) ");
        }

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return 0;
    }

    /**
     * Count total available entities (for pagination).
     */
    public int countAvailableEntities(State state) throws SQLException {
        // Reuse the same filter logic but COUNT instead of SELECT
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();

        sql.append("""
                SELECT COUNT(DISTINCT e.global_id)
                FROM entities e
                JOIN collections c ON e.collection_global_id = c.global_id
                WHERE c.dataset_id = ? AND c.original_id = ?
                """);
        params.add(state.getDatasetId());
        params.add(state.getCurrentCollectionId());

        // Same filter logic as getAvailableEntities...
        appendFilterClauses(sql, params, state);

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next())
                    return rs.getInt(1);
            }
        }
        return 0;
    }

    /**
     * Get facets (attribute -> value -> count) for available entities.
     */
    public Map<String, List<Map<String, Object>>> getFacets(State state) throws SQLException {
        Map<String, List<Map<String, Object>>> facets = new LinkedHashMap<>();

        // First, get the available entity IDs as a subquery
        StringBuilder subquery = new StringBuilder();
        List<Object> params = new ArrayList<>();

        subquery.append("""
                SELECT DISTINCT e.global_id
                FROM entities e
                JOIN collections c ON e.collection_global_id = c.global_id
                WHERE c.dataset_id = ? AND c.original_id = ?
                """);
        params.add(state.getDatasetId());
        params.add(state.getCurrentCollectionId());

        appendFilterClauses(subquery, params, state);

        // Now count facets
        String sql = "SELECT m.key, m.value, COUNT(*) as cnt FROM metadata m WHERE m.entity_id IN ("
                + subquery + ") GROUP BY m.key, m.value ORDER BY m.key, cnt DESC";

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("key");
                    String value = rs.getString("value");
                    int count = rs.getInt("cnt");

                    facets.computeIfAbsent(key, k -> new ArrayList<>());

                    Map<String, Object> facetValue = new HashMap<>();
                    facetValue.put("value", value);
                    facetValue.put("count", count);
                    facetValue.put("active", isFilterActive(state, key, value));
                    facets.get(key).add(facetValue);
                }
            }
        }
        return facets;
    }

    /**
     * Get reference facets: for each (targetCollection, reason) pair reachable
     * from the currently filtered entities, list the individual target entities
     * with their counts.
     *
     * Returns a map keyed by "collectionId:reason" where each value is a map
     * containing collectionId, collectionName, reason, and a list of entities.
     */
    public Map<String, Map<String, Object>> getReferenceFacets(State state) throws SQLException {
        Map<String, Map<String, Object>> facets = new LinkedHashMap<>();

        // Build the filtered-entity subquery (same as getFacets)
        StringBuilder subquery = new StringBuilder();
        List<Object> params = new ArrayList<>();

        subquery.append("""
                SELECT DISTINCT e.global_id
                FROM entities e
                JOIN collections c ON e.collection_global_id = c.global_id
                WHERE c.dataset_id = ? AND c.original_id = ?
                """);
        params.add(state.getDatasetId());
        params.add(state.getCurrentCollectionId());

        appendFilterClauses(subquery, params, state);

        // Join references against the filtered entities to find target entities
        String sql = """
                SELECT rc.original_id AS collection_id, rc.name AS collection_name,
                       r.reason, te.original_id AS entity_id, te.name AS entity_name,
                       COUNT(*) AS cnt
                FROM reference r
                JOIN entities te ON r.target_id = te.global_id
                JOIN collections rc ON te.collection_global_id = rc.global_id
                WHERE r.source_id IN (""" + subquery + """
                ) AND rc.dataset_id = ?
                GROUP BY rc.original_id, rc.name, r.reason, te.original_id, te.name
                ORDER BY rc.name, r.reason, cnt DESC, te.name
                """;
        params.add(state.getDatasetId());

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            // Track entity lists separately to avoid unchecked casts
            Map<String, List<Map<String, Object>>> entityLists = new HashMap<>();

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    int colId = rs.getInt("collection_id");
                    String colName = rs.getString("collection_name");
                    String reason = rs.getString("reason");
                    String key = colId + ":" + reason;

                    if (!facets.containsKey(key)) {
                        Map<String, Object> g = new LinkedHashMap<>();
                        g.put("collectionId", colId);
                        g.put("collectionName", colName);
                        g.put("reason", reason);
                        List<Map<String, Object>> entList = new ArrayList<>();
                        g.put("entities", entList);
                        facets.put(key, g);
                        entityLists.put(key, entList);
                    }

                    List<Map<String, Object>> entities = entityLists.get(key);

                    Map<String, Object> entity = new HashMap<>();
                    entity.put("id", rs.getInt("entity_id"));
                    entity.put("name", rs.getString("entity_name"));
                    entity.put("count", rs.getInt("cnt"));

                    // Check if this specific reference filter is already active
                    // Set<Integer> activeIds = state.getRfilters().get(key);
                    // entity.put("active", activeIds != null &&
                    // activeIds.contains(rs.getInt("entity_id")));

                    entities.add(entity);
                }
            }
        }
        return facets;
    }

    /**
     * Get available link destinations.
     */
    public List<Map<String, Object>> getAvailableLinks(State state) throws SQLException {
        List<Map<String, Object>> links = new ArrayList<>();

        StringBuilder subquery = new StringBuilder();
        List<Object> params = new ArrayList<>();

        subquery.append("""
                SELECT DISTINCT e.global_id
                FROM entities e
                JOIN collections c ON e.collection_global_id = c.global_id
                WHERE c.dataset_id = ? AND c.original_id = ?
                """);
        params.add(state.getDatasetId());
        params.add(state.getCurrentCollectionId());

        appendFilterClauses(subquery, params, state);

        String sql = "SELECT DISTINCT rc.original_id as collection_id, rc.name as collection_name, r.reason "
                + "FROM reference r "
                + "JOIN entities te ON r.target_id = te.global_id "
                + "JOIN collections rc ON te.collection_global_id = rc.global_id "
                + "WHERE r.source_id IN (" + subquery + ") "
                + "AND rc.dataset_id = ? "
                + "ORDER BY rc.name, r.reason";
        params.add(state.getDatasetId());

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> link = new HashMap<>();
                    link.put("collectionId", rs.getInt("collection_id"));
                    link.put("collectionName", rs.getString("collection_name"));
                    link.put("reason", rs.getString("reason"));
                    links.add(link);
                }
            }
        }
        return links;
    }

    /**
     * Get entity details.
     */
    public Map<String, Object> getEntityDetails(int datasetId, int collectionId, int entityId)
            throws SQLException {

        Map<String, Object> entity = new HashMap<>();

        String sql = """
                SELECT e.global_id, e.original_id, e.name, e.contents,
                       c.original_id as collection_id, c.name as collection_name
                FROM entities e
                JOIN collections c ON e.collection_global_id = c.global_id
                WHERE c.dataset_id = ? AND c.original_id = ? AND e.original_id = ?
                """;

        try (Connection conn = DatabaseService.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, datasetId);
            stmt.setInt(2, collectionId);
            stmt.setInt(3, entityId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    entity.put("id", rs.getInt("original_id"));
                    entity.put("name", rs.getString("name"));
                    entity.put("contents", rs.getString("contents"));

                    Map<String, Object> collection = new HashMap<>();
                    collection.put("id", rs.getInt("collection_id"));
                    collection.put("name", rs.getString("collection_name"));
                    entity.put("collection", collection);

                    // Get metadata
                    entity.put("metadata", getEntityMetadata(conn, rs.getInt("global_id")));

                    // Get references
                    entity.put("references", getEntityReferences(conn, rs.getInt("global_id")));
                }
            }
        }
        return entity;
    }

    /**
     * Get entities matching a union entry's filters.
     */
    // --- Helper methods ---

    private void appendStateFilters(StringBuilder sql, List<Object> params, State state, int depth) {
        String e = depth == 0 ? "e" : "e" + depth;
        String m = "m" + depth;
        String r = "r" + depth;
        String re = "re" + depth;
        String rc = "rc" + depth;

        // Metadata filters
        for (var entry : state.getMfilters().entrySet()) {
            String attr = entry.getKey();
            for (String val : entry.getValue()) {
                sql.append(" AND EXISTS (SELECT 1 FROM metadata " + m + " WHERE " + m + ".entity_id = " + e
                        + ".global_id AND " + m + ".key = ? AND " + m + ".value = ?) ");
                params.add(attr);
                params.add(val);
            }
        }

        // NOT metadata filters
        for (var entry : state.getNotMfilters().entrySet()) {
            String attr = entry.getKey();
            for (String val : entry.getValue()) {
                sql.append(" AND NOT EXISTS (SELECT 1 FROM metadata " + m + " WHERE " + m + ".entity_id = " + e
                        + ".global_id AND " + m + ".key = ? AND " + m + ".value = ?) ");
                params.add(attr);
                params.add(val);
            }
        }

        // Reference filters
        for (var entry : state.getRfilters().entrySet()) {
            int refColId = entry.getKey();
            for (var ref_entry : state.getRfilters().get(refColId).entrySet()) {
                String reason = ref_entry.getKey();
                for (Integer refEntId : ref_entry.getValue()) {
                    sql.append(" AND EXISTS (SELECT 1 FROM reference " + r + " JOIN entities " + re + " ON " + r
                            + ".target_id = " + re + ".global_id JOIN collections " + rc + " ON " + re
                            + ".collection_global_id = " + rc + ".global_id WHERE " + r + ".source_id = " + e
                            + ".global_id AND " + rc + ".original_id = ? AND " + rc + ".dataset_id = ? AND " + r
                            + ".reason = ? AND " + re + ".original_id = ?) ");
                    params.add(refColId);
                    params.add(state.getDatasetId());
                    params.add(reason);
                    params.add(refEntId);
                }
            }
        }

        // NOT Reference filters
        for (var entry : state.getNotRfilters().entrySet()) {
            int refColId = entry.getKey();
            for (var ref_entry : state.getNotRfilters().get(refColId).entrySet()) {
                String reason = ref_entry.getKey();
                for (Integer refEntId : ref_entry.getValue()) {
                    sql.append(" AND NOT EXISTS (SELECT 1 FROM reference " + r + " JOIN entities " + re + " ON " + r
                            + ".target_id = " + re + ".global_id JOIN collections " + rc + " ON " + re
                            + ".collection_global_id = " + rc + ".global_id WHERE " + r + ".source_id = " + e
                            + ".global_id AND " + rc + ".original_id = ? AND " + rc + ".dataset_id = ? AND " + r
                            + ".reason = ? AND " + re + ".original_id = ?) ");
                    params.add(refColId);
                    params.add(state.getDatasetId());
                    params.add(reason);
                    params.add(refEntId);
                }
            }
        }
    }

    private void appendFilterClauses(StringBuilder sql, List<Object> params, State state) {
        if (state.getLinks() != null && !state.getLinks().isEmpty()) {
            appendFilterClauses(sql, params, state, state.getLinks(), state.getLinks().size() - 1, 0);
        } else {
            appendStateFilters(sql, params, state, 0);
        }
    }

    private void appendFilterClauses(StringBuilder sql, List<Object> params, State state, List<Link> linkList) {
        if (linkList != null && !linkList.isEmpty()) {
            appendFilterClauses(sql, params, state, linkList, linkList.size() - 1, 0);
        } else {
            appendStateFilters(sql, params, state, 0);
        }
    }

    private void appendFilterClauses(StringBuilder sql, List<Object> params, State state, List<Link> linkList,
            int index, int depth) {

        appendStateFilters(sql, params, state, depth);

        if (index < 0) {
            return;
        }

        Link link = linkList.get(index);

        String e = depth == 0 ? "e" : "e" + depth;
        String next_e = "e" + (depth + 1);
        String next_c = "c" + (depth + 1);
        String r_link = "rl" + depth; // Reference table alias for the link

        if (link.isForward()) {
            sql.append(" AND " + e + ".global_id IN (SELECT " + r_link + ".target_id FROM reference " + r_link
                    + " JOIN entities " + next_e + " ON " + r_link + ".source_id = " + next_e + ".global_id "
                    + " JOIN collections " + next_c + " ON " + next_e + ".collection_global_id = " + next_c
                    + ".global_id WHERE " + next_c + ".dataset_id = ? AND " + next_c + ".original_id = ? AND " + r_link
                    + ".reason = ? ");
            params.add(state.getDatasetId());
            params.add(link.getState().getCurrentCollectionId());
            params.add(link.getReason());
        } else {
            sql.append(" AND " + e + ".global_id IN (SELECT " + r_link + ".source_id FROM reference " + r_link
                    + " JOIN entities " + next_e + " ON " + r_link + ".target_id = " + next_e + ".global_id "
                    + " JOIN collections " + next_c + " ON " + next_e + ".collection_global_id = " + next_c
                    + ".global_id WHERE " + next_c + ".dataset_id = ? AND " + next_c + ".original_id = ? AND " + r_link
                    + ".reason = ? ");
            params.add(state.getDatasetId());
            params.add(link.getState().getCurrentCollectionId());
            params.add(link.getReason());
        }

        appendFilterClauses(sql, params, link.getState(), linkList, index - 1, depth + 1);
        sql.append(") ");
    }

    /**
     * Append filter clauses from a union entry (saved filter state).
     * This method was removed or deprecated in favor of passing State objects
     * directly.
     */
    // private void appendUnionEntryFilters(...) { ... }

    private boolean isFilterActive(State state, String key, String value) {
        Set<String> vals = state.getMfilters().get(key);
        return vals != null && vals.contains(value);
    }

    private Map<String, List<String>> getEntityMetadata(Connection conn, int globalId)
            throws SQLException {
        Map<String, List<String>> metadata = new LinkedHashMap<>();

        String sql = "SELECT key, value FROM metadata WHERE entity_id = ? ORDER BY key, value";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, globalId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    metadata.computeIfAbsent(rs.getString("key"), k -> new ArrayList<>())
                            .add(rs.getString("value"));
                }
            }
        }
        return metadata;
    }

    private Map<String, List<Map<String, Object>>> getEntityReferences(Connection conn, int globalId)
            throws SQLException {
        Map<String, List<Map<String, Object>>> references = new LinkedHashMap<>();

        String sql = """
                SELECT r.reason, te.original_id, te.name, c.original_id as collection_id
                FROM reference r
                JOIN entities te ON r.target_id = te.global_id
                JOIN collections c ON te.collection_global_id = c.global_id
                WHERE r.source_id = ?
                ORDER BY r.reason, te.name
                """;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, globalId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String reason = rs.getString("reason");
                    references.computeIfAbsent(reason, k -> new ArrayList<>());

                    Map<String, Object> ref = new HashMap<>();
                    ref.put("id", rs.getInt("original_id"));
                    ref.put("name", rs.getString("name"));
                    ref.put("collectionId", rs.getInt("collection_id"));
                    references.get(reason).add(ref);
                }
            }
        }
        return references;
    }
}
