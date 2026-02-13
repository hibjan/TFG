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

/**
 * GET /api/union - Get entities from all union set entries
 */
@WebServlet("/api/union")
public class UnionServlet extends HttpServlet {

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

        List<State> unionSet = smanager.getUnionSet();

        if (unionSet.isEmpty()) {
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            mapper.writeValue(resp.getWriter(), Map.of(
                    "unionEntries", Collections.emptyList(),
                    "message", "No union entries saved"));
            return;
        }

        try {
            int page = 0;
            int size = 20;

            try {
                String pageParam = req.getParameter("page");
                if (pageParam != null) {
                    page = Integer.parseInt(pageParam);
                }
                String sizeParam = req.getParameter("size");
                if (sizeParam != null) {
                    size = Integer.parseInt(sizeParam);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid params, use defaults
            }

            List<Map<String, Object>> entities = dao.getUnionEntities(unionSet, page, size);
            int total = dao.countUnionEntities(unionSet);

            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");

            Map<String, Object> responseMap = new LinkedHashMap<>();
            responseMap.put("entities", entities);
            responseMap.put("total", total);

            mapper.writeValue(resp.getWriter(), responseMap);

        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }

    }
}
