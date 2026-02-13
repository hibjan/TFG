package io.github.hibjan.tfg.controller;

import io.github.hibjan.tfg.dao.NavigationDAO;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * GET /api/datasets - List available datasets
 */
@WebServlet("/api/datasets")
public class DatasetsServlet extends HttpServlet {

    private final ObjectMapper mapper = new ObjectMapper();
    private final NavigationDAO dao = new NavigationDAO();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        try {
            List<Map<String, Object>> datasets = dao.getDatasets();

            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            mapper.writeValue(resp.getWriter(), Map.of("datasets", datasets));

        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
}
