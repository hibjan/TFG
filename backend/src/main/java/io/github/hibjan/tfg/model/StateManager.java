package io.github.hibjan.tfg.model;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.ArrayList;

public class StateManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private int datasetId;

    private Deque<State> historyStack = new ArrayDeque<>();
    private State cur;

    private List<State> unionSet = new ArrayList<>();

    private List<Link> linkList = new ArrayList<>();
    private Deque<Link> linkStack = new ArrayDeque<>();

    public StateManager(int datasetID, int collectionID) {
        this.datasetId = datasetID;
        this.historyStack = new ArrayDeque<>();
        this.cur = new State(datasetID, collectionID);
    }

    public State getCurrent() {
        return this.cur;
    }

    public void addMetadataFilter(String element, String value) {
        this.cur.addMetadataFilter(element, value);
    }

    public void removeMetadataFilter(String element, String value) {
        this.cur.removeMetadataFilter(element, value);
    }

    public void addReferenceFilter(int env, String reason, int value) {
        this.cur.addReferenceFilter(env, reason, value);
    }

    public void removeReferenceFilter(int env, String reason, int value) {
        this.cur.removeReferenceFilter(env, reason, value);
    }

    public void addNotMetadataFilter(String element, String value) {
        this.cur.addNotMetadataFilter(element, value);
    }

    public void removeNotMetadataFilter(String element, String value) {
        this.cur.removeNotMetadataFilter(element, value);
    }

    public void addNotReferenceFilter(int env, String reason, int value) {
        this.cur.addNotReferenceFilter(env, reason, value);
    }

    public void removeNotReferenceFilter(int env, String reason, int value) {
        this.cur.removeNotReferenceFilter(env, reason, value);
    }

    public void link(int env, String reason) {
        this.linkList.add(new Link(true, this.cur.getCurrentCollectionId(), reason, new State(this.cur)));
        this.cur.link(env, reason);
        this.cur.setLinks(new ArrayList<>(this.linkList));
        this.linkStack.push(this.linkList.get(this.linkList.size() - 1));
    }

    public void goback() {
        Link last = this.linkStack.pop();
        this.linkList.add(new Link(false, this.cur.getCurrentCollectionId(), last.getReason(), new State(this.cur)));
        this.cur.goback(last.getEnv());
        this.cur.setLinks(new ArrayList<>(this.linkList));
    }

    public void restore() {
        if (this.historyStack.size() > 1) {
            this.historyStack.pop();
            this.cur = this.historyStack.pop();
            this.linkList = new ArrayList<>(this.cur.getLinks());
        }
    }

    public void save() {
        this.historyStack.push(new State(this.cur));
    }

    public void union(int collectionId) {
        this.cur.setLinks(new ArrayList<>(this.linkList));
        this.unionSet.add(this.cur);
        change(collectionId);
    }

    public void change(int collectionId) {
        this.historyStack.clear();
        this.linkList.clear();
        this.cur = new State(this.datasetId, collectionId);
    }

    public Integer getDatasetId() {
        return this.datasetId;
    }

    public Integer getCurrentCollectionId() {
        return this.cur.getCurrentCollectionId();
    }

    public List<State> getUnionSet() {
        return this.unionSet;
    }

    public List<Link> getLinkList() {
        return this.linkList;
    }
}
