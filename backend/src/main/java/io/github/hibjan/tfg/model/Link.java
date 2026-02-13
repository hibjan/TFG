package io.github.hibjan.tfg.model;

import java.io.Serializable;

public class Link implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean forward;
    private int env;
    private String reason;
    private State state;

    public Link(boolean forward, int env, String reason, State state) {
        this.forward = forward;
        this.env = env;
        this.reason = reason;
        this.state = state;
    }

    public boolean isForward() {
        return forward;
    }

    public int getEnv() {
        return env;
    }

    public String getReason() {
        return reason;
    }

    public State getState() {
        return state;
    }

}
