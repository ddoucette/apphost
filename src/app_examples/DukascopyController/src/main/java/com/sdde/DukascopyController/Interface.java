
package com.sdde.DukascopyController;

import org.zeromq.*;
import java.util.ArrayList;


public class Interface implements Runnable {

    private Llog log;
    private InterfaceEvent ievt;
    private boolean alive = true;
    private ArrayList<ZSocket> sockets;

    protected ZMQ.Socket[] pipe;
    protected ZMQ.Poller poller;
    protected ZContext ctxt;

    public Interface(InterfaceEvent ievt)
    {
        this.log = new Llog("Interface");
        this.ievt = ievt;
        this.ctxt = new ZContext();
        this.sockets = new ArrayList();
        this.pipe = ZHelpers.zpipe(ctxt);
        this.poller = this.ctxt.getContext().poller();
        assert this.poller != null;
        this.poller.register(this.pipe[1], ZMQ.Poller.POLLIN);
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
                int i = 1;
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

    public void add_socket (ZSocket zsocket)
    {
        assert zsocket != null;
        assert zsocket.get_socket() != null;
        assert this.find_socket(zsocket.get_location()) == null;

        this.sockets.add(zsocket);
        this.poller.register(zsocket.get_socket(), ZMQ.Poller.POLLIN);
    }

    public void remove_socket(ZSocket zsocket)
    {
        for (ZSocket z: this.sockets)
        {
            if (z.get_location().equals(zsocket.get_location()))
            {
                this.sockets.remove(zsocket);
                return;
            }
        }
        assert false;
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
        this.pipe[0].send(msg, 0);
    }

    public void close()
    {
        this.alive = false;
    }
}
