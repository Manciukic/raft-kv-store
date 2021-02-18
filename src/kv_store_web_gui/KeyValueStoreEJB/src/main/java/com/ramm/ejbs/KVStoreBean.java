package com.ramm.ejbs;

import com.ericsson.otp.erlang.*;
import com.ramm.interfaces.KeyValueStore;

import javax.ejb.Stateless;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Stateless(name = "KVStoreEJB")
public class KVStoreBean implements KeyValueStore {
    private Logger log = Logger.getLogger(KVStoreBean.class.getName());

    //configuration parameters
    private static final String serverNodeShortName = "raft";
    private static final String serverRegisteredName = "kv_store"; //configuration parameter
    private static final String clientNodeShortNamePattern = "jinterface%d"; //configuration parameter
    private static final String cookie="raft";

    // erlang constant values (initialized in constructor)
    private final OtpNode clientNode;
    private final OtpMbox mbox;
    private final String serverNodeName;

    // unique id
    private static final AtomicInteger counter = new AtomicInteger(0);

    //constructor
    public KVStoreBean() {
        int myId = counter.getAndIncrement();

        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "localhost";
        }

        serverNodeName = serverNodeShortName + "@" + hostname;

        String clientNodeShortName = String.format(clientNodeShortNamePattern, myId);
        String clientNodeName = clientNodeShortName + "@" + hostname;
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
        log.info("Created mailbox "+ mbox.getName());
    }

    private OtpErlangObject genServerCall(String method, OtpErlangObject arguments){
        OtpErlangObject ref = clientNode.createRef();

        OtpErlangObject[] payload = new OtpErlangObject[2];
        payload[0] = new OtpErlangAtom(method);
        payload[1] = arguments;

        OtpErlangObject[] tag = new OtpErlangObject[2];
        tag[0] = mbox.self();
        tag[1] = ref;

        OtpErlangObject[] msg = new OtpErlangObject[3];
        msg[0] = new OtpErlangAtom("$gen_call");
        msg[1] = new OtpErlangTuple(tag);
        msg[2] = new OtpErlangTuple(payload);

        mbox.send(serverRegisteredName, serverNodeName, new OtpErlangTuple(msg));
        log.info("Call " + method + " on " + serverRegisteredName + " at " + serverNodeName);
        try {
            OtpErlangObject recvRef;
            OtpErlangObject recvPayload;
            do {
                OtpErlangObject reply = mbox.receive(5000);
                if (reply == null)
                    return null;
                log.info(reply.toString());
                OtpErlangTuple tupleReply = (OtpErlangTuple) reply;
                recvRef = tupleReply.elementAt(0);
                recvPayload = tupleReply.elementAt(1);
            } while (!ref.equals(recvRef)); // ignore replies to other requests

            return recvPayload;
        } catch (OtpErlangExit otpErlangExit) {
            otpErlangExit.printStackTrace();
            return null;
        } catch (OtpErlangDecodeException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String get(String key) {

        OtpErlangString erlKey = new OtpErlangString(key);
        OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[]{erlKey});

        OtpErlangObject msg = genServerCall("get", req);
        if (msg == null)
            return null;

        //getting the message content
        OtpErlangString ErlStr = (OtpErlangString) msg;//it is supposed to be a string...
        return ErlStr.stringValue();
    }

    @Override
    public Map<String, String> getAll() {
        OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[]{});

        OtpErlangObject msg = genServerCall("get_all", req);
        if (msg == null)
            return null;

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
        OtpErlangString erlKey = new OtpErlangString(key);
        OtpErlangString erlVal = new OtpErlangString(value);
        OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[]{erlKey,erlVal});

        OtpErlangObject msg = genServerCall("set", req);
        if (msg == null)
            return false;

        //getting the message content
        OtpErlangAtom ErlStr = (OtpErlangAtom) msg;//it is supposed to be a tuple...
        if (ErlStr.atomValue().equals("ok"))
            return true;
        else
            return false;
    }

    @Override
    public boolean delete(String key) {
        OtpErlangString erlKey = new OtpErlangString(key);
        OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[]{erlKey});

        OtpErlangObject msg = genServerCall("delete", req);
        if (msg == null)
            return false;

        //getting the message content
        OtpErlangAtom erlReply = (OtpErlangAtom) msg;
        if (erlReply.atomValue().equals("ok"))
            return true;
        else
            return false;

    }

    @Override
    public boolean deleteAll() {
        OtpErlangTuple req = new OtpErlangTuple(new OtpErlangObject[]{});

        OtpErlangObject msg = genServerCall("delete_all", req);
        if (msg == null)
            return false;

        //getting the message content
        OtpErlangAtom erlReply = (OtpErlangAtom) msg;
        if (erlReply.atomValue().equals("ok"))
            return true;
        else
            return false;
    }
}
