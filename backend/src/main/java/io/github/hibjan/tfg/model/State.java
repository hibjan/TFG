package io.github.hibjan.tfg.model;

import java.io.Serializable;
import java.util.*;

public class State implements Serializable {
    private static final long serialVersionUID = 1L;

    private static int datasetId;
    private int currentCollectionId;

    // ENV (1=Movies) ->
    // Filter (Genre) ->
    // Values (Action, Drama, ...)
    private HashMap<Integer, HashMap<String, HashSet<String>>> mfilters = new HashMap<>();
    private HashMap<Integer, HashMap<String, HashSet<String>>> notMfilters = new HashMap<>();

    // ENV (1=Movies) ->
    // REFERENCE_ENV (2=People) ->
    // Filter (Director) ->
    // Values (108, 350, ...)
    private HashMap<Integer, HashMap<Integer, HashMap<String, HashSet<Integer>>>> rfilters = new HashMap<>();
    private HashMap<Integer, HashMap<Integer, HashMap<String, HashSet<Integer>>>> notRfilters = new HashMap<>();

    public State(int datasetId, int collectionId) {
        State.datasetId = datasetId;
        this.currentCollectionId = collectionId;
    }

    public State(State other) {
        this.currentCollectionId = other.currentCollectionId;
        this.mfilters = mfiltersDeepCopy(other.mfilters);
        this.notMfilters = mfiltersDeepCopy(other.notMfilters);
        this.rfilters = rfiltersDeepCopy(other.rfilters);
        this.notRfilters = rfiltersDeepCopy(other.notRfilters);
        this.links = new ArrayList<>(other.links);
    }

    private List<Link> links = new ArrayList<>();

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

    public void link(int targetCollectionId, String reason) {
        this.currentCollectionId = targetCollectionId;
    }

    public void goback(int targetCollectionId) {
        this.currentCollectionId = targetCollectionId;
    }

    public int getDatasetId() {
        return datasetId;
    }

    public int getCurrentCollectionId() {
        return currentCollectionId;
    }

    public HashMap<String, HashSet<String>> getMfilters() {
        if (!mfilters.containsKey(currentCollectionId)) {
            return new HashMap<>();
        }
        return mfilters.get(currentCollectionId);
    }

    public HashMap<String, HashSet<String>> getNotMfilters() {
        if (!notMfilters.containsKey(currentCollectionId)) {
            return new HashMap<>();
        }
        return notMfilters.get(currentCollectionId);
    }

    public HashMap<Integer, HashMap<String, HashSet<Integer>>> getRfilters() {
        if (!rfilters.containsKey(currentCollectionId)) {
            return new HashMap<>();
        }
        return rfilters.get(currentCollectionId);
    }

    public HashMap<Integer, HashMap<String, HashSet<Integer>>> getNotRfilters() {
        if (!notRfilters.containsKey(currentCollectionId)) {
            return new HashMap<>();
        }
        return notRfilters.get(currentCollectionId);
    }

    // --- Helpers ---

    private void helperAddMetadataFilter(HashMap<Integer, HashMap<String, HashSet<String>>> mfilters, String attribute,
            String value) {
        if (!mfilters.containsKey(currentCollectionId)) {
            mfilters.put(currentCollectionId, new HashMap<>());
        }
        if (!mfilters.get(currentCollectionId).containsKey(attribute)) {
            mfilters.get(currentCollectionId).put(attribute, new HashSet<>());
        }
        mfilters.get(currentCollectionId).get(attribute).add(value);
    }

    private void helperRemoveMetadataFilter(HashMap<Integer, HashMap<String, HashSet<String>>> mfilters,
            String attribute, String value) {
        if (mfilters.containsKey(currentCollectionId) &&
                mfilters.get(currentCollectionId).containsKey(attribute)) {
            mfilters.get(currentCollectionId).get(attribute).remove(value);
            if (mfilters.get(currentCollectionId).get(attribute).isEmpty()) {
                mfilters.get(currentCollectionId).remove(attribute);
                if (mfilters.get(currentCollectionId).isEmpty()) {
                    mfilters.remove(currentCollectionId);
                }
            }
        }
    }

    private void helperAddReferenceFilter(
            HashMap<Integer, HashMap<Integer, HashMap<String, HashSet<Integer>>>> rfilters,
            Integer env,
            String reason, Integer value) {
        if (!rfilters.containsKey(currentCollectionId)) {
            rfilters.put(currentCollectionId, new HashMap<>());
        }
        if (!rfilters.get(currentCollectionId).containsKey(env)) {
            rfilters.get(currentCollectionId).put(env, new HashMap<>());
        }
        if (!rfilters.get(currentCollectionId).get(env).containsKey(reason)) {
            rfilters.get(currentCollectionId).get(env).put(reason, new HashSet<>());
        }
        rfilters.get(currentCollectionId).get(env).get(reason).add(value);
    }

    private void helperRemoveReferenceFilter(
            HashMap<Integer, HashMap<Integer, HashMap<String, HashSet<Integer>>>> rfilters, Integer env, String reason,
            Integer value) {
        if (rfilters.containsKey(currentCollectionId) &&
                rfilters.get(currentCollectionId).containsKey(env) &&
                rfilters.get(currentCollectionId).get(env).containsKey(reason) &&
                rfilters.get(currentCollectionId).get(env).get(reason).contains(value)) {
            rfilters.get(currentCollectionId).get(env).get(reason).remove(value);
            if (rfilters.get(currentCollectionId).get(env).get(reason).isEmpty()) {
                rfilters.get(currentCollectionId).get(env).remove(reason);
                if (rfilters.get(currentCollectionId).get(env).isEmpty()) {
                    rfilters.get(currentCollectionId).remove(env);
                    if (rfilters.get(currentCollectionId).isEmpty()) {
                        rfilters.remove(currentCollectionId);
                    }
                }
            }
        }
    }

    private HashMap<Integer, HashMap<String, HashSet<String>>> mfiltersDeepCopy(
            HashMap<Integer, HashMap<String, HashSet<String>>> mfilters) {
        HashMap<Integer, HashMap<String, HashSet<String>>> mfilters_copy = new HashMap<>();
        for (Integer env : mfilters.keySet()) {
            mfilters_copy.put(env, new HashMap<>());
            for (String attribute : mfilters.get(env).keySet()) {
                mfilters_copy.get(env).put(attribute,
                        new HashSet<>(mfilters.get(env).get(attribute)));
            }
        }

        return mfilters_copy;
    }

    private HashMap<Integer, HashMap<Integer, HashMap<String, HashSet<Integer>>>> rfiltersDeepCopy(
            HashMap<Integer, HashMap<Integer, HashMap<String, HashSet<Integer>>>> rfilters) {
        HashMap<Integer, HashMap<Integer, HashMap<String, HashSet<Integer>>>> rfilters_copy = new HashMap<>();
        for (Integer env : rfilters.keySet()) {
            rfilters_copy.put(env, new HashMap<>());
            for (Integer reference_env : rfilters.get(env).keySet()) {
                rfilters_copy.get(env).put(reference_env, new HashMap<>());
                for (String reason : rfilters.get(env).get(reference_env).keySet()) {
                    rfilters_copy.get(env).get(reference_env).put(reason,
                            new HashSet<>(rfilters.get(env).get(reference_env).get(reason)));
                }
            }
        }

        return rfilters_copy;
    }
}
