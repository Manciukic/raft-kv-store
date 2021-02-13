package com.ramm.servlets;

import com.ramm.models.DataManager;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

public class DataServlet extends HttpServlet {

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (request.getParameter("key")!=null){
            String key = request.getParameter("key");
            request.setAttribute("requestedValue", DataManager.get(key));    // Get
        }

        request.setAttribute("kvData", DataManager.loadData());       // Get all

        RequestDispatcher dispatcher = request.getRequestDispatcher("/WEB-INF/views/data.jsp");
        dispatcher.forward(request, response);
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (request.getParameter("set") != null){
            String key = request.getParameter("key");
            String value = request.getParameter("value");

            // Send result to GUI
            request.setAttribute("setOpResult", DataManager.set(key, value));

            // Reload the datatable
            request.setAttribute("kvData", DataManager.loadData());

            RequestDispatcher dispatcher = request.getRequestDispatcher("/WEB-INF/views/data.jsp");
            dispatcher.forward(request, response);
        }
        else if (request.getParameter("delete") != null){
            String key = request.getParameter("key");
            DataManager.delete(key);

            // Send result to GUI
            request.setAttribute("deleteOpResult", DataManager.delete(key));

            // Reload the datatable
            request.setAttribute("kvData", DataManager.loadData());

            RequestDispatcher dispatcher = request.getRequestDispatcher("/WEB-INF/views/data.jsp");
            dispatcher.forward(request, response);
        }
        else if(request.getParameter("deleteAll")!=null){
            DataManager.deleteAll();

            // Send result to GUI
            request.setAttribute("deleteAllOpResult", DataManager.deleteAll());

            // Reload the datatable
            request.setAttribute("kvData", DataManager.loadData());

            RequestDispatcher dispatcher = request.getRequestDispatcher("/WEB-INF/views/data.jsp");
            dispatcher.forward(request, response);
        }
    }
}
