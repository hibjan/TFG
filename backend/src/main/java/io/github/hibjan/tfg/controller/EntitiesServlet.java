package io.github.hibjan.tfg.controller;

import io.github.hibjan.tfg.dao.NavigationDAO;
import io.github.hibjan.tfg.model.StateManager;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

@WebServlet("/api/entities")
public class EntitiesServlet extends HttpServlet {

    private final ObjectMapper mapper = new ObjectMapper();
    private final NavigationDAO dao = new NavigationDAO();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        HttpSession session = req.getSession(false);
        if (session == null || session.getAttribute("navState") == null) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "No navigation state. Select a dataset first.");
            return;
        }

        StateManager smanager = (StateManager) session.getAttribute("navState");

        int page = parseInt(req.getParameter("page"), 0);
        int size = parseInt(req.getParameter("size"), 50);
        size = Math.min(size, 200);

        try {
            List<Map<String, Object>> entities = dao.getAvailableEntities(smanager.getCurrent(), page, size);
            int total = dao.countAvailableEntities(smanager.getCurrent());

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("entities", entities);
            result.put("total", total);
            result.put("page", page);
            result.put("size", size);
            result.put("collectionId", smanager.getCurrentCollectionId());

            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            mapper.writeValue(resp.getWriter(), result);

        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private int parseInt(String val, int defaultVal) {
        if (val == null)
            return defaultVal;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }
}
