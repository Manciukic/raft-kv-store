package com.ramm.ejbs;

import com.ericsson.otp.erlang.*;
import com.ramm.interfaces.KeyValueStore;

import javax.ejb.Stateless;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Stateless(name = "KVStoreEJB")
public class KVStoreBean implements KeyValueStore {
    private static final String serverNodeName = "node1@honeypot"; //configuration parameter
    private static final String serverRegisteredName = "kv_store"; //configuration parameter
    private static final String clientNodeName = "jinterface@honeypot"; //configuration parameter
    private static final String cookie="";
    private OtpNode clientNode;  //initialized in constructor
    private final OtpMbox mbox;

    //constructor
    public KVStoreBean() {
        try {
            if (!cookie.equals("")) {
                clientNode = new OtpNode(clientNodeName, cookie);
            }
            else {
                clientNode = new OtpNode(clientNodeName);
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("IOException while creating new OTP Node", e);
        }
        mbox = clientNode.createMbox("client_mbox_");
        System.out.println("Created mailbox "+ mbox.getName());

    }

    @Override
    public String get(String key) {

        OtpErlangString Erlkey = new OtpErlangString(key);
        OtpErlangString get = new OtpErlangString("get");
        OtpErlangTuple arg = new OtpErlangTuple(Erlkey);
        OtpErlangTuple reqMsg = new OtpErlangTuple(new OtpErlangObject[]{this.mbox.self(), get,arg});

        //sending out the request
        mbox.send(serverRegisteredName, serverNodeName, reqMsg);
        //blocking receive operation
        OtpErlangObject msg = null;
        try {
            msg = mbox.receive(5000);
        } catch (OtpErlangExit otpErlangExit) {
            otpErlangExit.printStackTrace();
            return null;
        } catch (OtpErlangDecodeException e) {
            e.printStackTrace();
            return null;
        }
        //getting the message content
        OtpErlangString ErlStr = (OtpErlangString) msg;//it is supposed to be a tuple...
        String value=ErlStr.stringValue();

        return value;
    }

    @Override
    public Map<String, String> getAll() {

        OtpErlangString get = new OtpErlangString("get_all");

        OtpErlangTuple Arg = new OtpErlangTuple(new OtpErlangObject[]{});
        OtpErlangTuple reqMsg = new OtpErlangTuple(new OtpErlangObject[]{this.mbox.self(), get,Arg});

        //sending out the request
        mbox.send(serverRegisteredName, serverNodeName, reqMsg);
        //blocking receive operation
        OtpErlangObject msg = null;
        try {
            msg = mbox.receive(5000);
        } catch (OtpErlangExit otpErlangExit) {
            otpErlangExit.printStackTrace();
            return null;
        } catch (OtpErlangDecodeException e) {
            e.printStackTrace();
            return null;
        }
        //getting the message content
        OtpErlangList erlList = (OtpErlangList) msg;
        Map<String, String> values = new HashMap<String, String>();

        for (OtpErlangObject erlO : erlList) {
            OtpErlangTuple t = (OtpErlangTuple) erlO;
            values.put(t.elementAt(0).toString(), t.elementAt(1).toString());
        }
        return values;

    }

    @Override
    public boolean set(String key, String value) {

        OtpErlangString Erlkey = new OtpErlangString(key);

        OtpErlangString Erlval = new OtpErlangString(value);
        OtpErlangTuple arg = new OtpErlangTuple(new OtpErlangObject[]{Erlkey,Erlval});
        OtpErlangString set = new OtpErlangString("set");

        OtpErlangTuple reqMsg = new OtpErlangTuple(new OtpErlangObject[]{this.mbox.self(), set,arg});

        //sending out the request
        mbox.send(serverRegisteredName, serverNodeName, reqMsg);
        //blocking receive operation
        OtpErlangObject msg = null;
        try {
            msg = mbox.receive(5000);
        } catch (OtpErlangExit otpErlangExit) {
            otpErlangExit.printStackTrace();
            return false;
        } catch (OtpErlangDecodeException e) {
            e.printStackTrace();
            return false;
        }
        //getting the message content
        OtpErlangAtom ErlStr = (OtpErlangAtom) msg;//it is supposed to be a tuple...
        if (ErlStr.atomValue().equals("ok"))
            return true;
        else
            return false;
    }

    @Override
    public boolean delete(String key) {
        OtpErlangString Erlkey = new OtpErlangString(key);
        OtpErlangString del = new OtpErlangString("delete");

        OtpErlangTuple reqMsg = new OtpErlangTuple(new OtpErlangObject[]{this.mbox.self(), del,Erlkey});

        //sending out the request
        mbox.send(serverRegisteredName, serverNodeName, reqMsg);
        //blocking receive operation
        OtpErlangObject msg = null;
        try {
            msg = mbox.receive(5000);
        } catch (OtpErlangExit otpErlangExit) {
            otpErlangExit.printStackTrace();
        } catch (OtpErlangDecodeException e) {
            e.printStackTrace();
        }
        //getting the message content
        OtpErlangAtom Erlresp = (OtpErlangAtom) msg;
        if (Erlresp.atomValue().equals("ok"))
            return true;
        else
            return false;

    }

    @Override
    public boolean deleteAll() {

        OtpErlangString del = new OtpErlangString("delete_all");

        OtpErlangTuple reqMsg = new OtpErlangTuple(new OtpErlangObject[]{this.mbox.self(), del,});

        //sending out the request
        mbox.send(serverRegisteredName, serverNodeName, reqMsg);
        //blocking receive operation
        OtpErlangObject msg = null;
        try {
            msg = mbox.receive(5000);
        } catch (OtpErlangExit otpErlangExit) {
            otpErlangExit.printStackTrace();
        } catch (OtpErlangDecodeException e) {
            e.printStackTrace();
        }
        //getting the message content
        OtpErlangAtom Erlresp = (OtpErlangAtom) msg;
        if (Erlresp.atomValue().equals("ok"))
            return true;
        else
            return false;
    }
}
