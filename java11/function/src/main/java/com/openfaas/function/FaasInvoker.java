package com.openfaas.function;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.net.ssl.HttpsURLConnection;

import org.apache.storm.tuple.Tuple;

public class FaasInvoker {

    protected class Invocation {
        public String url;
        public Tuple tuple;
        public int retry;

        public Invocation(String url, Tuple tuple, int retry) {
            this.url = url;
            this.tuple = tuple;
            this.retry = retry;
        }
    }

    private ConcurrentLinkedQueue<Invocation> pendingInvocations;
    private boolean stopped = true;
    private Thread invocationThread = null;

    private String callBackEndPoint;

    protected boolean invoke(String url, Tuple tuple) {
        // Create HTTP request from tuple content
        String msg = TupleSerializer.dumpTuple(tuple);
        HttpURLConnection connection = null;
        try {
            // Create connection
            URL httpurl = new URL(url);
            connection = (HttpURLConnection) httpurl.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            connection.setRequestProperty("Content-Length", Integer.toString(msg.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            connection.setHeader

            // Send request
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(msg);
            wr.close();

            // Get Response
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            StringBuffer response = new StringBuffer(); // or StringBuffer if Java version 5+
            String line;
            while ((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            rd.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String getFunctionUrl(Integer taskId) {
        return null;
    }

    public void call(String url, Collection<Tuple> tuples) {
        for (Tuple tuple : tuples) {
            call(url, tuple);
        }
    }

    public void call(Integer taskId, Collection<Tuple> tuples) {
        // Call a function given task Id
        String funcUrl = getFunctionUrl(taskId);
        if (funcUrl != null) {
            call(funcUrl, tuples);
        }
    }

    public void call(String url, Tuple tuple) {
        pendingInvocations.add(new Invocation(url, tuple, 10));
    }

    public void start() {
        this.run();
        this.stopped = false;
    }

    protected void run() {
        this.invocationThread = new Thread() {
            public void run() {
                while (!stopped) {
                    if (!pendingInvocations.isEmpty()) {
                        Invocation inv = pendingInvocations.poll();
                        if (inv != null) {
                            boolean success = invoke(inv.url, inv.tuple);
                            if (!success && inv.retry > 0) {
                                inv.retry--;
                                pendingInvocations.add(inv);
                            }
                        }
                    } else {
                        // Suspend for a while if there is no new invocations
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
        this.invocationThread.start();
    }

    public void stop() {
        // Telling the invocation thead to stop
        this.stopped = true;
        try {
            this.invocationThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            this.stopped = false;
        }
    }

    public boolean isStopped() {
        return this.stopped;
    }
    
}