package io.github.hibjan.tfg.controller;

import io.github.hibjan.tfg.dao.NavigationDAO;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * GET /api/collections?datasetId=X - List collections for a dataset
 */
@WebServlet("/api/collections")
public class CollectionsServlet extends HttpServlet {

    private final ObjectMapper mapper = new ObjectMapper();
    private final NavigationDAO dao = new NavigationDAO();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String datasetIdParam = req.getParameter("datasetId");
        if (datasetIdParam == null) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing datasetId parameter");
            return;
        }

        try {
            int datasetId = Integer.parseInt(datasetIdParam);
            List<Map<String, Object>> collections = dao.getCollections(datasetId);

            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            mapper.writeValue(resp.getWriter(), Map.of("collections", collections));

        } catch (NumberFormatException e) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid datasetId");
        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
}
