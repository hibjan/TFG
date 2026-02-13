package io.github.hibjan.tfg.controller;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.hibjan.tfg.model.StateManager;

import com.fasterxml.jackson.core.type.TypeReference;

@WebServlet("/api/navigation")
public class NavigationServlet extends HttpServlet {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        HttpSession session = req.getSession(true);

        Map<String, Object> body = mapper.readValue(req.getInputStream(), new TypeReference<Map<String, Object>>() {
        });
        String action = (String) body.get("action");

        if (action == null) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing action");
            return;
        }

        try {
            StateManager smanager = (StateManager) session.getAttribute("navState");
            smanager.save();

            switch (action) {
                case "add_mfilter" -> {
                    smanager.addMetadataFilter(
                            getString(body, "attribute"),
                            getString(body, "value"));
                }

                case "rm_mfilter" -> {
                    smanager.removeMetadataFilter(
                            getString(body, "attribute"),
                            getString(body, "value"));
                }

                case "add_not_mfilter" -> {
                    smanager.addNotMetadataFilter(
                            getString(body, "attribute"),
                            getString(body, "value"));
                }

                case "rm_not_mfilter" -> {
                    smanager.removeNotMetadataFilter(
                            getString(body, "attribute"),
                            getString(body, "value"));
                }

                case "add_rfilter" -> {
                    smanager.addReferenceFilter(
                            getInt(body, "collectionId"),
                            getString(body, "reason"),
                            getInt(body, "entityId"));
                }

                case "rm_rfilter" -> {
                    smanager.removeReferenceFilter(
                            getInt(body, "collectionId"),
                            getString(body, "reason"),
                            getInt(body, "entityId"));
                }

                case "add_not_rfilter" -> {
                    smanager.addNotReferenceFilter(
                            getInt(body, "collectionId"),
                            getString(body, "reason"),
                            getInt(body, "entityId"));
                }

                case "rm_not_rfilter" -> {
                    smanager.removeNotReferenceFilter(
                            getInt(body, "collectionId"),
                            getString(body, "reason"),
                            getInt(body, "entityId"));
                }

                case "link" -> {
                    smanager.link(
                            getInt(body, "collectionId"),
                            getString(body, "reason"));
                }

                case "goback" -> {
                    smanager.goback();
                }

                case "restore" -> {
                    smanager.restore();
                }

                case "union" -> {
                    int newCollectionId = getInt(body, "collectionId");
                    smanager.union(newCollectionId);
                }

                default -> {
                    resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown action: " + action);
                    return;
                }
            }

            // Return success with current state info
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("success", true);
            if (smanager != null) {
                result.put("collectionId", smanager.getCurrentCollectionId());
            }
            mapper.writeValue(resp.getWriter(), result);

        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private String getString(Map<String, Object> body, String key) {
        return (String) body.get(key);
    }

    private int getInt(Map<String, Object> body, String key) {
        Object val = body.get(key);
        if (val instanceof Integer)
            return (Integer) val;
        if (val instanceof String)
            return Integer.parseInt((String) val);
        throw new IllegalArgumentException("Missing or invalid: " + key);
    }
}
