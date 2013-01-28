package com.sdde.DukascopyController;

import java.util.Collection;
import org.zeromq.*;


public class ZSocket {

    private Llog log;

    protected ZMQ.Socket socket;
    protected ZContext ctxt;
    protected String location;
    protected String protocol_name;
    protected int socket_type;

    private static final byte[] EMPTY = new byte[0];

    public ZSocket (int socket_type)
    {
        assert socket_type >= ZMQ.PUB;
        this.log = new Llog("ZSocket");
        this.socket_type = socket_type;
        this.socket = null;
        this.location = null;
        this.protocol_name = null;
        this.ctxt = new ZContext();
        assert this.ctxt != null;
    }

    public void close()
    {
        if (this.ctxt != null)
        {
            this.ctxt.destroy();
            this.ctxt = null;
        }
    }

    public void create_socket()
    {
        this.socket = this.ctxt.createSocket(this.socket_type);
        assert this.socket != null;
    }

    public void subscribe(String str)
    {
        assert this.socket != null;
        this.socket.subscribe(str.getBytes());
    }

    public String recv ()
    {
        assert this.socket != null;

        byte msg[] = this.socket.recv(0);
        String str = "";

        if ( msg.length > 0 )
            str = new String(msg);

        this.log.debug("Received: " + str);
        return str;
    }

    public String recv (byte[] address)
    {
        assert this.socket != null;
        assert this.socket_type == ZMQ.ROUTER;

        String addr = new String(this.socket.recv(0));
        this.log.debug("Received address: " + addr);
        byte empty[] = this.socket.recv(0);
        this.log.debug("Received empty:");
        byte msg[] = this.socket.recv(0);
        String str = "";

        if ( msg.length > 0 )
            str = new String(msg);

        this.log.debug("Received: " + str);
        return str;
    }

    public void send (String msg)
    {
        assert this.socket != null;
        assert msg != null;

        this.log.debug("Sending: " + msg);
        this.socket.send(msg.getBytes(), 0);
    }

    public void send (byte[] address, String msg)
    {
        assert this.socket != null;
        assert this.socket_type == ZMQ.ROUTER;
        assert msg != null;

        this.log.debug("Sending to (" + address + "): " + msg);
        this.socket.send(address, ZMQ.SNDMORE);
        this.socket.send(EMPTY, ZMQ.SNDMORE);
        this.socket.send(msg.getBytes(), 0);
    }
}
