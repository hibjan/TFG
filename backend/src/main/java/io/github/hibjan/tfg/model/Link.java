package io.github.hibjan.tfg.model;

import java.io.Serializable;

public class Link implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean forward;
    private int collectionId;
    private String reason;
    private State state;

    public Link(boolean forward, int collectionId, String reason, State state) {
        this.forward = forward;
        this.collectionId = collectionId;
        this.reason = reason;
        this.state = state;
    }

    public boolean isForward() {
        return forward;
    }

    public int getCollectionId() {
        return collectionId;
    }

    public String getReason() {
        return reason;
    }

    public State getState() {
        return state;
    }

}
