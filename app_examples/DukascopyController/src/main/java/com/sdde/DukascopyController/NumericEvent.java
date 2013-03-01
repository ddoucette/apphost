
package com.sdde.DukascopyController;

import org.zeromq.*;


public class NumericEvent extends EventSource {

    private Llog log;

    public NumericEvent(String event_name)
    {
        super(event_name,
              "NUMERIC",
              AppSystem.GetUserName(),
              AppSystem.GetApplicationName());
        this.log = new Llog("NumericEvent:" + event_name);
    }

    public void send (int value)
    {
        this.send(Integer.toString(value));
    }

    public void send (double value)
    {
        this.send(Double.toString(value));
    }

    /*
     * Tests:
     */
    public static void main ( String[] args )
    {
        boolean assertions_enabled = false;
        assert assertions_enabled = true;

        if ( assertions_enabled == false )
            throw new RuntimeException("Asserts must be enabled!!!");

        test1();
    }

    private static void test1()
    {
        final Llog log = new Llog("test1()");

        String user_name = "user123";
        String application_name = "test1";
        AppSystem.Init(user_name, application_name);

        String event_name = "myval1";

        ZSocketServer s = new ZSocketServer(ZMQ.PULL,
                                          "ipc",
                                          user_name + ":" + application_name);
        s.bind();

        NumericEvent evt = new NumericEvent(event_name);
        assert evt != null;

        int nr_events = 100;
        for (int i = 0; i < nr_events; i++ )
        {
            evt.send(i);
        }

        for (int i = 0; i < nr_events; i++ )
        {
            String msg = s.recv();
            String[] msgs = msg.split(" ");

            String tx_msg = "" + i;
            assert msgs.length == 6;
            assert msgs[5].equals(tx_msg);
        }

        evt.close();
        s.close();
        log.info("PASSED");
    }
}
