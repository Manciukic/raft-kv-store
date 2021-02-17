package com.ramm.servlets;

import com.ramm.interfaces.KeyValueStore;
import com.ramm.models.DataManager;

import javax.ejb.EJB;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@WebServlet(name = "dataServlet", urlPatterns = "/data")
public class DataServlet extends HttpServlet {
    @EJB(name = "KVStoreEJB")
    private KeyValueStore kvStore;

    private DataManager dataManager;

    @Override
    public void init() throws ServletException {
        dataManager = new DataManager(kvStore);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (request.getParameter("key")!=null){
            String key = request.getParameter("key");
            request.setAttribute("requestedValue", dataManager.get(key));    // Get
        }

        request.setAttribute("kvData", dataManager.loadData());       // Get all

        RequestDispatcher dispatcher = request.getRequestDispatcher("/WEB-INF/views/data.jsp");
        dispatcher.forward(request, response);
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (request.getParameter("set") != null){
            String key = request.getParameter("key");
            String value = request.getParameter("value");

            // Send result to GUI
            request.setAttribute("setOpResult", dataManager.set(key, value));

            // Reload the datatable
            request.setAttribute("kvData", dataManager.loadData());

            RequestDispatcher dispatcher = request.getRequestDispatcher("/WEB-INF/views/data.jsp");
            dispatcher.forward(request, response);
        }
        else if (request.getParameter("delete") != null){
            String key = request.getParameter("key");
            dataManager.delete(key);

            // Send result to GUI
            request.setAttribute("deleteOpResult", dataManager.delete(key));

            // Reload the datatable
            request.setAttribute("kvData", dataManager.loadData());

            RequestDispatcher dispatcher = request.getRequestDispatcher("/WEB-INF/views/data.jsp");
            dispatcher.forward(request, response);
        }
        else if(request.getParameter("deleteAll")!=null){
            dataManager.deleteAll();

            // Send result to GUI
            request.setAttribute("deleteAllOpResult", dataManager.deleteAll());

            // Reload the datatable
            request.setAttribute("kvData", dataManager.loadData());

            RequestDispatcher dispatcher = request.getRequestDispatcher("/WEB-INF/views/data.jsp");
            dispatcher.forward(request, response);
        }
    }
}
