
package com.sdde.DukascopyController;

import org.zeromq.*;


public class ZHelpers {

    public static final int HWM_DEFAULT = 1;
    public static final int LINGER_DEFAULT = 0;

    private ZHelpers () {};

    public static ZMQ.Socket[] zpipe(ZContext ctxt, int hwm)
    {
        ZMQ.Socket[] sockets = new ZMQ.Socket[2];

        for ( int i = 0; i < 2; i++ )
        {
            sockets[i] = ctxt.createSocket(ZMQ.PAIR);
            assert sockets[i] != null;
            sockets[i].setLinger(LINGER_DEFAULT);
            sockets[i].setHWM(hwm);
        }
        String sock_name = String.format("inproc://pipe-%d",
                                         sockets[0].hashCode());
        sockets[0].bind(sock_name);
        sockets[1].connect(sock_name);
        return sockets;
    }

    public static ZMQ.Socket[] zpipe(ZContext ctxt)
    {
        return zpipe(ctxt, HWM_DEFAULT);
    }

    public static void sleep (int msec)
    {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
        }
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
