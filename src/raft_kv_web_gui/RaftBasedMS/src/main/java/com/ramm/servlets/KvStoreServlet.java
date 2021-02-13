package com.ramm.servlets;

import com.ramm.models.DataManager;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.json.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;

public class KvStoreServlet extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        JSONObject objectToSend = new JSONObject();

        if (request.getParameter("key")!=null){         // Get
            String key = request.getParameter("key");
            objectToSend = DataManager.jsonGet(key);
        }
        else
        {                                                   // Get all
            objectToSend = DataManager.jsonGetAll();
        }

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        PrintWriter writer=response.getWriter();
        writer.write(objectToSend.toString());
        writer.flush();
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        JSONObject objectToSend = new JSONObject();
        if (request.getParameter("set") != null){
            JSONObject receivedJSONObject = new JSONObject(request.getParameter(""));
            objectToSend = DataManager.set(receivedJSONObject);
        }
        else if (request.getParameter("delete") != null){
            JSONObject receivedJSONObject = new JSONObject(request.getParameter(""));
            objectToSend = DataManager.delete(receivedJSONObject);
        }
        else if(request.getParameter("deleteAll")!=null){
            objectToSend = DataManager.jsonDeleteAll();
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
