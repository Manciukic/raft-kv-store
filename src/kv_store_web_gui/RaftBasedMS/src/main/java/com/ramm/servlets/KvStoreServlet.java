package com.ramm.servlets;

import com.ramm.interfaces.KeyValueStore;
import com.ramm.models.DataManager;
import org.json.JSONObject;

import javax.ejb.EJB;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

@WebServlet(name = "kvstoreServlet", urlPatterns = "/kvstore")
public class KvStoreServlet extends HttpServlet {
    @EJB(name = "KVStoreEJB")
    private KeyValueStore kvStore;

    private DataManager dataManager;

    @Override
    public void init() throws ServletException {
        dataManager = new DataManager(kvStore);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        JSONObject objectToSend = new JSONObject();

        if (request.getParameter("key")!=null){         // Get
            String key = request.getParameter("key");
            objectToSend = dataManager.jsonGet(key);
        }
        else
        {                                                   // Get all
            objectToSend = dataManager.jsonGetAll();
        }

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        PrintWriter writer=response.getWriter();
        writer.write(objectToSend.toString());
        writer.flush();
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        JSONObject objectToSend = new JSONObject();
        if (request.getParameter("set") != null){
            JSONObject receivedJSONObject = new JSONObject(request.getParameter(""));
            objectToSend = dataManager.set(receivedJSONObject);
        }
        else if (request.getParameter("delete") != null){
            JSONObject receivedJSONObject = new JSONObject(request.getParameter(""));
            objectToSend = dataManager.delete(receivedJSONObject);
        }
        else if(request.getParameter("deleteAll")!=null){
            objectToSend = dataManager.jsonDeleteAll();
        }
        else{
            // Invalid operation name
            objectToSend.put("response", false);
            objectToSend.put("message", "Invalid operation name: valid operations are get, jsonGetAll, set, delete and deleteAll");
        }

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        PrintWriter writer=response.getWriter();
        writer.write(objectToSend.toString());
        writer.flush();
    }

    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
    }
}
