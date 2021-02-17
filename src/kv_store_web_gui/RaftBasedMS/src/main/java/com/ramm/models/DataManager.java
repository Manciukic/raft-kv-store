package com.ramm.models;

import com.ramm.interfaces.KeyValueStore;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DataManager {
    private KeyValueStore kvStore;

    public DataManager(KeyValueStore kvStore) {
        this.kvStore = kvStore;
    }

    public String loadData(){
        Map<String, String> values= new HashMap<>();
        values = getAll();
        Iterator it = values.entrySet().iterator();

        String data = "";
        while(it.hasNext()) {
            Map.Entry mapElement = (Map.Entry)it.next();
            data += "<tr>" +
                        "<td data-column=\"Key\">" + String.valueOf(mapElement.getKey()) + "</td>" +
                        "<td data-column=\"Value\">" + String.valueOf(mapElement.getValue()) + "</td>" +
                        "<td data-column=\"Delete\">" +
                            "<form action='data' method='post'>" +
                                "<input name=\"key\" value=\""+String.valueOf(mapElement.getKey())+"\" hidden>" +
                                "<button name='delete' type=\"submit\" class=\"btn btn-danger\">Delete</button>" +
                            "</form>" +
                        "</td>" +
                    "</tr>";
        }
        return data;
    }

    public String get(String key){
        return kvStore.get(key);      // <TODO> To integrate
    }

    public Map<String, String> getAll(){
        return kvStore.getAll();      // <TODO> To integrate
    }

    public boolean set(String key, String value){
        return kvStore.set(key, value);      // <TODO> To integrate (also the result of operation, should be a boolean)
    }

    public boolean delete(String key){
        return kvStore.delete(key);      // <TODO> To integrate (also the result of operation, should be a boolean)
    }

    public boolean deleteAll(){
        return kvStore.deleteAll();      // <TODO> To integrate (also the result of operation, should be a boolean)
    }

    // FUNCTIONS FOR JSON COMMUNICATIONS
    public JSONObject jsonGet(String key){
        String value = "";
        JSONObject objectToReturn;

        value = kvStore.get(key);     // <TODO> To integrate

        objectToReturn = new JSONObject();
        if(value != ""){
            objectToReturn.put("success", true);
            objectToReturn.put("data", value);
        }
        else {
            objectToReturn.put("success", false);
        }

        return objectToReturn;
    }

    public JSONObject jsonGetAll(){
        Map<String, String> allPairs = new HashMap<String,String>();
        JSONObject objectToReturn;

        allPairs = kvStore.getAll();      // <TODO> To integrate

        objectToReturn = new JSONObject();
        if(allPairs!=null){
            Iterator it = allPairs.entrySet().iterator();
            JSONArray dataArray = new JSONArray();
            JSONObject currentElement;

            while(it.hasNext()) {
                currentElement = new JSONObject();

                Map.Entry mapElement = (Map.Entry)it.next();
                currentElement.put(String.valueOf(mapElement.getKey()), String.valueOf(mapElement.getValue()));
                dataArray.put(currentElement);
            }

            objectToReturn.put("success", true);
            objectToReturn.put("data", dataArray);
        }
        else {
            objectToReturn.put("success", false);
        }

        return objectToReturn;
    }

    public JSONObject set(JSONObject dataObject){
        JSONObject objectToReturn = new JSONObject();
        String key = dataObject.getString("key");
        String value = dataObject.getString("value");

        if(kvStore.set(key, value)){          // <TODO> To integrate (also the result of operation, should be a boolean)
            objectToReturn.put("success", true);
        }
        else{
            objectToReturn.put("success", false);
        }

        return objectToReturn;
    }

    public JSONObject delete(JSONObject dataObject){
        JSONObject objectToReturn = new JSONObject();
        String key = dataObject.getString("key");

        if(kvStore.delete(key)){          // <TODO> To integrate (also the result of operation, should be a boolean)
            objectToReturn.put("success", true);
        }
        else{
            objectToReturn.put("success", false);
        }

        return objectToReturn;
    }

    public JSONObject jsonDeleteAll(){
        JSONObject objectToReturn = new JSONObject();

        if(kvStore.deleteAll()){      // <TODO> To integrate (also the result of operation, should be a boolean)
            objectToReturn.put("success", true);
        }
        else{
            objectToReturn.put("success", false);
        }

        return objectToReturn;
    }
}
