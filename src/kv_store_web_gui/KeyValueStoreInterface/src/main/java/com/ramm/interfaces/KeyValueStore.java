package com.ramm.interfaces;

import javax.ejb.Remote;
import java.util.Map;

@Remote
public interface KeyValueStore {
        String get(String key);
        Map<String, String> getAll();
        boolean set(String key, String values);
        boolean delete(String key);
        boolean deleteAll();
}
