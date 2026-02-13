package io.github.hibjan.tfg.controller;

import io.github.hibjan.tfg.model.StateManager;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * POST /api/session - Initialize session for a dataset
 * GET /api/session - Get current session info
 */
@WebServlet("/api/session")
public class SessionServlet extends HttpServlet {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        Map<String, Object> body = mapper.readValue(req.getInputStream(), new TypeReference<Map<String, Object>>() {
        });

        Object datasetIdObj = body.get("datasetId");
        Object collectionIdObj = body.get("collectionId");

        if (datasetIdObj == null || collectionIdObj == null) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing datasetId or collectionId");
            return;
        }

        int datasetId = datasetIdObj instanceof Integer ? (Integer) datasetIdObj
                : Integer.parseInt(datasetIdObj.toString());
        int collectionId = collectionIdObj instanceof Integer ? (Integer) collectionIdObj
                : Integer.parseInt(collectionIdObj.toString());

        HttpSession session = req.getSession(true);
        StateManager smanager = new StateManager(datasetId, collectionId);
        session.setAttribute("navState", smanager);

        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sessionId", session.getId());
        result.put("datasetId", datasetId);
        result.put("collectionId", collectionId);
        mapper.writeValue(resp.getWriter(), result);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        HttpSession session = req.getSession(false);

        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        Map<String, Object> result = new LinkedHashMap<>();

        if (session != null && session.getAttribute("navState") != null) {
            StateManager smamager = (StateManager) session.getAttribute("navState");
            result.put("active", true);
            result.put("sessionId", session.getId());
            result.put("datasetId", smamager.getDatasetId());
            result.put("collectionId", smamager.getCurrentCollectionId());
        } else {
            result.put("active", false);
        }

        mapper.writeValue(resp.getWriter(), result);
    }
}