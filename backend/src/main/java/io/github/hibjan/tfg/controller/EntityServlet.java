package io.github.hibjan.tfg.controller;

import io.github.hibjan.tfg.dao.NavigationDAO;
import io.github.hibjan.tfg.model.StateManager;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

@WebServlet("/api/entity")
public class EntityServlet extends HttpServlet {

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

        StateManager sManager = (StateManager) session.getAttribute("navState");

        String idParam = req.getParameter("id");
        if (idParam == null) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing id parameter");
            return;
        }

        try {
            int entityId = Integer.parseInt(idParam);
            int collectionId = sManager.getCurrentCollectionId();

            String colParam = req.getParameter("collectionId");
            if (colParam != null) {
                try {
                    collectionId = Integer.parseInt(colParam);
                } catch (NumberFormatException e) {
                    // Ignore invalid collectionId, stick to current
                }
            }

            Map<String, Object> entity = dao.getEntityDetails(sManager.getDatasetId(),
                    collectionId, entityId);

            if (entity.isEmpty()) {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Entity not found");
                return;
            }

            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            mapper.writeValue(resp.getWriter(), entity);

        } catch (NumberFormatException e) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid id");
        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
}
