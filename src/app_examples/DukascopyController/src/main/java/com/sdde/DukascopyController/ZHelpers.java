
package com.sdde.DukascopyController;

import org.zeromq.*;


public class ZHelpers {

    private ZHelpers () {};

    public static ZMQ.Socket[] zpipe(ZContext ctxt)
    {
        ZMQ.Socket[] sockets = new ZMQ.Socket[2];

        for ( int i = 0; i < 2; i++ )
        {
            sockets[i] = ctxt.createSocket(ZMQ.PAIR);
            assert sockets[i] != null;
            sockets[i].setLinger(0);
            sockets[i].setHWM(1);
        }
        String sock_name = String.format("inproc://pipe-%d",
                                         sockets[0].hashCode());
        sockets[0].bind(sock_name);
        sockets[1].connect(sock_name);
        return sockets;
    }

    public static void main ( String[] args )
    {
        ZHelpers.test1();
    }

    public static void test1()
    {
        ZContext ctxt = new ZContext();

        ZMQ.Socket[] sockets = ZHelpers.zpipe(ctxt);
        String tx_msg = "hello there...";
        sockets[0].send(tx_msg, 0);
        String msg = sockets[1].recvStr(0);
        assert msg == tx_msg;

        sockets[1].send(tx_msg, 0);
        msg = sockets[0].recvStr(0);
        assert msg == tx_msg;

        System.out.println("test1() PASSED!\n");
    }
}
