package io.github.hibjan.tfg.model;

import java.io.Serializable;
import java.util.*;

public class State implements Serializable {
    private static final long serialVersionUID = 1L;

    private int datasetId;
    private int currentCollectionId;

    // Filter (Genre) -> Values (Action, Drama, ...)
    private HashMap<String, HashSet<String>> mfilters = new HashMap<>();
    private HashMap<String, HashSet<String>> notMfilters = new HashMap<>();

    // refCollectionId (2=People) ->
    // Filter (Director) ->
    // Values (108, 350, ...)
    private HashMap<Integer, HashMap<String, HashSet<Integer>>> rfilters = new HashMap<>();
    private HashMap<Integer, HashMap<String, HashSet<Integer>>> notRfilters = new HashMap<>();

    private List<Link> links = new ArrayList<>();

    public State(int datasetId, int collectionId) {
        this.datasetId = datasetId;
        this.currentCollectionId = collectionId;
    }

    public State(State other) {
        this.datasetId = other.datasetId;
        this.currentCollectionId = other.currentCollectionId;
        this.mfilters = mfiltersDeepCopy(other.mfilters);
        this.notMfilters = mfiltersDeepCopy(other.notMfilters);
        this.rfilters = rfiltersDeepCopy(other.rfilters);
        this.notRfilters = rfiltersDeepCopy(other.notRfilters);
        this.links = new ArrayList<>(other.links);
    }

    public List<Link> getLinks() {
        return links;
    }

    public void setLinks(List<Link> links) {
        this.links = links;
    }

    // --- Filter Operations ---

    public void addMetadataFilter(String attribute, String value) {
        helperAddMetadataFilter(mfilters, attribute, value);
    }

    public void removeMetadataFilter(String attribute, String value) {
        helperRemoveMetadataFilter(mfilters, attribute, value);
    }

    public void addNotMetadataFilter(String attribute, String value) {
        helperAddMetadataFilter(notMfilters, attribute, value);
    }

    public void removeNotMetadataFilter(String attribute, String value) {
        helperRemoveMetadataFilter(notMfilters, attribute, value);
    }

    public void addReferenceFilter(int refCollectionId, String reason, int entityId) {
        helperAddReferenceFilter(rfilters, refCollectionId, reason, entityId);
    }

    public void removeReferenceFilter(int refCollectionId, String reason, int entityId) {
        helperRemoveReferenceFilter(rfilters, refCollectionId, reason, entityId);
    }

    public void addNotReferenceFilter(int refCollectionId, String reason, int entityId) {
        helperAddReferenceFilter(notRfilters, refCollectionId, reason, entityId);
    }

    public void removeNotReferenceFilter(int refCollectionId, String reason, int entityId) {
        helperRemoveReferenceFilter(notRfilters, refCollectionId, reason, entityId);
    }

    // --- Link Navigation ---

    // Each path owns its own filters; crossing a link starts fresh, so
    // the filters of the previous path stay only in the Link snapshot.
    public void link(int targetCollectionId) {
        this.currentCollectionId = targetCollectionId;
        this.mfilters.clear();
        this.notMfilters.clear();
        this.rfilters.clear();
        this.notRfilters.clear();
    }

    public void goback(int targetCollectionId, State snapshot) {
        this.currentCollectionId = targetCollectionId;
        this.mfilters = mfiltersDeepCopy(snapshot.mfilters);
        this.notMfilters = mfiltersDeepCopy(snapshot.notMfilters);
        this.rfilters = rfiltersDeepCopy(snapshot.rfilters);
        this.notRfilters = rfiltersDeepCopy(snapshot.notRfilters);
    }

    public int getDatasetId() {
        return datasetId;
    }

    public int getCurrentCollectionId() {
        return currentCollectionId;
    }

    public HashMap<String, HashSet<String>> getMfilters() {
        return mfilters;
    }

    public HashMap<String, HashSet<String>> getNotMfilters() {
        return notMfilters;
    }

    public HashMap<Integer, HashMap<String, HashSet<Integer>>> getRfilters() {
        return rfilters;
    }

    public HashMap<Integer, HashMap<String, HashSet<Integer>>> getNotRfilters() {
        return notRfilters;
    }

    // --- Helpers ---

    private void helperAddMetadataFilter(HashMap<String, HashSet<String>> target, String attribute, String value) {
        target.computeIfAbsent(attribute, k -> new HashSet<>()).add(value);
    }

    private void helperRemoveMetadataFilter(HashMap<String, HashSet<String>> target, String attribute, String value) {
        HashSet<String> values = target.get(attribute);
        if (values == null)
            return;
        values.remove(value);
        if (values.isEmpty()) {
            target.remove(attribute);
        }
    }

    private void helperAddReferenceFilter(HashMap<Integer, HashMap<String, HashSet<Integer>>> target,
            Integer refCollectionId, String reason, Integer value) {
        target.computeIfAbsent(refCollectionId, k -> new HashMap<>())
                .computeIfAbsent(reason, k -> new HashSet<>())
                .add(value);
    }

    private void helperRemoveReferenceFilter(HashMap<Integer, HashMap<String, HashSet<Integer>>> target,
            Integer refCollectionId, String reason, Integer value) {
        HashMap<String, HashSet<Integer>> byReason = target.get(refCollectionId);
        if (byReason == null)
            return;
        HashSet<Integer> values = byReason.get(reason);
        if (values == null)
            return;
        values.remove(value);
        if (values.isEmpty()) {
            byReason.remove(reason);
            if (byReason.isEmpty()) {
                target.remove(refCollectionId);
            }
        }
    }

    private HashMap<String, HashSet<String>> mfiltersDeepCopy(HashMap<String, HashSet<String>> source) {
        HashMap<String, HashSet<String>> copy = new HashMap<>();
        for (var entry : source.entrySet()) {
            copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return copy;
    }

    private HashMap<Integer, HashMap<String, HashSet<Integer>>> rfiltersDeepCopy(
            HashMap<Integer, HashMap<String, HashSet<Integer>>> source) {
        HashMap<Integer, HashMap<String, HashSet<Integer>>> copy = new HashMap<>();
        for (var refEntry : source.entrySet()) {
            HashMap<String, HashSet<Integer>> byReason = new HashMap<>();
            for (var reasonEntry : refEntry.getValue().entrySet()) {
                byReason.put(reasonEntry.getKey(), new HashSet<>(reasonEntry.getValue()));
            }
            copy.put(refEntry.getKey(), byReason);
        }
        return copy;
    }
}
