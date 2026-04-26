package io.github.hibjan.tfg.controller;

import io.github.hibjan.tfg.dao.NavigationDAO;
import io.github.hibjan.tfg.model.State;
import io.github.hibjan.tfg.model.StateManager;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

@WebServlet("/api/facets/metadata")
public class MetadataFacetsServlet extends HttpServlet {

    private final ObjectMapper mapper = new ObjectMapper();
    private final NavigationDAO dao = new NavigationDAO();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        HttpSession session = req.getSession(false);
        if (session == null || session.getAttribute("navState") == null) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "No navigation state");
            return;
        }

        StateManager smanager = (StateManager) session.getAttribute("navState");

        try {
            Map<String, List<Map<String, Object>>> metadata = dao.getFacets(smanager.getCurrent());

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("metadata", metadata);
            result.put("activeFilters", getActiveFilters(smanager.getCurrent()));

            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            mapper.writeValue(resp.getWriter(), result);

        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private Map<String, Object> getActiveFilters(State state) {
        Map<String, Object> active = new LinkedHashMap<>();
        active.put("mfilters", state.getMfilters());
        active.put("notMfilters", state.getNotMfilters());
        return active;
    }
}
