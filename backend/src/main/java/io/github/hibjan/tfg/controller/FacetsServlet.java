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

@WebServlet("/api/facets")
public class FacetsServlet extends HttpServlet {

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
            Map<String, Map<String, Object>> references = dao.getReferenceFacets(smanager.getCurrent());
            List<Map<String, Object>> links = dao.getAvailableLinks(smanager.getCurrent());

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("metadata", metadata);
            result.put("references", references);
            result.put("links", links);
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
        active.put("rfilters", state.getRfilters());
        active.put("notRfilters", state.getNotRfilters());
        /*
         * if (state.hasActiveLink()) {
         * Map<String, Object> link = new HashMap<>();
         * link.put("sourceCollection", state.getLinkSourceCollectionId());
         * link.put("reason", state.getLinkReason());
         * active.put("activeLink", link);
         * }
         */
        return active;
    }
}
