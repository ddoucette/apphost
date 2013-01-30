
package com.sdde.DukascopyController;

import org.zeromq.*;
import java.util.ArrayList;


public class Interface extends Thread {

    private final int MAX_SOCKETS = 10;

    private Llog log;
    private InterfaceEvent ievt;
    private boolean alive = true;
    private ArrayList<ZSocket> sockets;

    protected ZMQ.Socket[] pipe;
    protected ZMQ.Poller poller;
    protected ZContext ctxt;

    public Interface(InterfaceEvent ievt)
    {
        assert ievt != null;

        this.log = new Llog("Interface");
        this.ievt = ievt;
        this.ctxt = new ZContext();
        this.sockets = new ArrayList(MAX_SOCKETS);
        this.pipe = ZHelpers.zpipe(ctxt);
        this.poller = this.ctxt.getContext().poller(MAX_SOCKETS);
        assert this.poller != null;
        this.poller.register(this.pipe[1], ZMQ.Poller.POLLIN);
        this.start();
    }

    @Override
    public void run()
    {
        while (!Thread.currentThread().isInterrupted() && this.alive )
        {
            this.poller.poll();
            if (this.poller.pollin(0))
            {
                /* Socket in position 0 is always our receive pipe. */
                this.process_command_pipe();
            }
            else
            {
                /*
                 * XXX
                 * This sucks.  N-squared in sockets to find the
                 * one with input.
                 */
                for ( int i = 1; i < MAX_SOCKETS; i++ )
                {
                    if ( this.poller.pollin(i) )
                    {
                        for ( ZSocket z : this.sockets )
                        {
                            if (z.get_socket() == this.poller.getSocket(i))
                                this.process_socket(z);
                        }
                    }
                }
            }
        }
        // We are done.  Remove all sockets from our list.
        for (ZSocket z : this.sockets)
            z.close();
        this.sockets.clear();
    }

    private void process_command_pipe()
    {
        String full_msg = this.pipe[1].recvStr(0);
        if ( full_msg == null )
            return;

        String[] msgs = full_msg.split(" ", 2);
        assert msgs.length >= 1;

        if ( msgs[0].equals("MSG") )
        {
            assert msgs.length == 2;

            // This is a command to be sent out the socket.
            // Make sure we have only 1 socket to send out, otherwise
            // this is a SW error.
            assert this.sockets.size() <= 1;

            ZSocket zsocket = this.sockets.get(0);
            assert zsocket != null;
            zsocket.send(msgs[1]);
        }
        else if ( msgs[0].equals("KILL") )
        {
            this.alive = false;
        }
        else if ( msgs[0].equals("PASS") )
        {
            /* 
             * We do nothing, this message is used to unblock the
             * polling thread.
             */
        }
        else
        {
            log.error("Invalid message header received: (" + msgs[0] + ")");
            assert false;
        }
    }

    private void process_socket(ZSocket zsocket)
    {
        String msg = zsocket.recv();
        if (msg == null)
            return;
        this.ievt.msg_cback(msg);
    }

    public void add_socket (ZSocket zsocket)
    {
        assert zsocket != null;
        assert zsocket.get_socket() != null;
        assert this.find_socket(zsocket.get_location()) == null;

        this.sockets.add(zsocket);
        this.poller.register(zsocket.get_socket(), ZMQ.Poller.POLLIN);
        /*
         * We must kick the socket polling thread to allow the poll
         * call to pick up this newly added socket.  We accomplish this
         * by sending a PASS message to the thread.
         */
        push_in_msg_raw("PASS");
    }

    public void remove_socket(ZSocket zsocket)
    {
        this.poller.unregister(zsocket.get_socket());
        this.sockets.remove(zsocket);
        push_in_msg_raw("PASS");
    }

    public ZSocket find_socket(String location)
    {
        for (ZSocket zsocket : this.sockets)
        {
            if (zsocket.get_location().equals(location))
            {
                return zsocket;
            }
        }
        return null;
    }

    public void push_in_msg(String msg)
    {
        String full_msg = "MSG ".concat(msg);
        push_in_msg_raw(full_msg);
    }

    private void push_in_msg_raw(String msg)
    {
        this.pipe[0].send(msg, 0);
    }

    public void close()
    {
        push_in_msg_raw("KILL");
    }

    public static void main ( String[] args )
    {
        test1();
    }

    private static void test1()
    {
        final Llog log = new Llog("test1()");

        class MyTestClass implements InterfaceEvent
        {
            public boolean passed = false;
            private Interface intf;
            private ZSocketServer server;

            public MyTestClass()
            {
                intf = new Interface(this);
                int[] port_range = {4321, 4323};
                server = new ZSocketServer(ZMQ.REP,
                                           "tcp",
                                           "*",
                                           port_range);

                // Bind first to create the socket.
                server.bind();
                intf.add_socket(server);
                passed = false;
            }

            public void msg_cback (String msg)
            {
                log.info("Received: " + msg);
                passed = true;
            }

            public void close ()
            {
                this.intf.close();
            }
        }

        MyTestClass mc = new MyTestClass();

        ZSocketClient c = new ZSocketClient(ZMQ.REQ,
                                          "tcp",
                                          "127.0.0.1",
                                          4321);
        c.connect();
        String tx_msg = "hello there...";
        c.send(tx_msg);

        // We need to pause for a few ms to allow the interface thread
        // to process the socket messages and forward them.
        ZHelpers.sleep(2000);

        assert mc.passed == true : "Did not receive the message!";
        mc.close();

        log.info("PASSED");
    }
}
