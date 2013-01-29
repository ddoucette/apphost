
package com.sdde.DukascopyController;

import java.util.ArrayList;
import org.zeromq.*;


public class ZSocketClient extends ZSocket {

    private String address;
    private int port;
    private Llog log;

    public ZSocketClient (  int socket_type,
                            String protocol_name,
                            String address,
                            int port )
    {
        super (socket_type);
        assert protocol_name == "tcp";
        assert port > 0 && port < 65535;
        this.log = new Llog("ZSocketClient");
        this.port = port;
        this.address = address;
        this.protocol_name = protocol_name;
    }

    public ZSocketClient (  int socket_type,
                            String protocol_name,
                            String address )
    {
        super (socket_type);
        assert protocol_name == "ipc";
        this.log = new Llog("ZSocketClient");
        this.address = address;
        this.protocol_name = protocol_name;
    }


    public void connect()
    {
        this.create_socket();
        this.location = this.protocol_name + "://" + this.address;

        if (this.protocol_name == "tcp")
            this.location += ":" + this.port;
        this.log.debug("Connecting to: " + this.location);
        this.socket.connect(this.location);
    }

	public static void main(final String[] args) throws Exception
    {
        ZSocketClient.test1();
        // ZSocketClient.test2();
        ZSocketClient.test3();
        ZSocketClient.test4();
	}

    public static void test1()
    {
        // Simple socket server/client.  Basic verification
        int port_range[] = {4321,4323};
        ZSocketServer s = new ZSocketServer(ZMQ.REP,
                                          "tcp",
                                          "127.0.0.1",
                                          port_range);
        ZSocketClient c = new ZSocketClient(ZMQ.REQ,
                                          "tcp",
                                          "127.0.0.1",
                                          4321);
        s.bind();
        c.connect();
        String tx_msg = "hello there...";
        c.send(tx_msg);
        String msg = s.recv();
        assert msg == tx_msg;
        c.close();
        s.close();
        System.out.println("test1() PASSED!\n");
    }

    private static void test2()
    {

        // Multiple requestors with a simple protocol header
        ArrayList<ZSocketClient> clist = new ArrayList();
        int nr_clients = 10;

        int port_range[] = {4321,4323};
        ZSocketServer s = new ZSocketServer(ZMQ.REP,
                                          "tcp",
                                          "*",
                                          port_range);
        assert s != null;
        s.bind();

        int i = 0;
        while (i < nr_clients)
        {
            ZSocketClient c = new ZSocketClient(ZMQ.REQ,
                                                "tcp",
                                                "127.0.0.1",
                                                4321);
            assert c != null;
            clist.add(c);
            c.connect();
            i++;
        }

        String tx_msg = "hello there...";
        i = 0;
        while (i < nr_clients)
        {
            String msg = tx_msg + ":" + i;
            ZSocketClient c = clist.get(i);
            c.send(msg);
            i++;
        }

        i = 0;
        while (i < nr_clients)
        {
            byte[] address = new byte[1];
            String msg_text = s.recv(address);
            msg_text += " - processed";
            s.send(address, msg_text);
            i++;
        }

        i = 0;
        while (i < nr_clients)
        {
            String rx_msg = tx_msg + ":" + i + " - processed";
            ZSocketClient c = clist.get(i);
            String msg = c.recv();
            assert msg == rx_msg;
            c.close();
            i++;
        }

        s.close();
        System.out.println("test2() - PASSED");
    }

    public static void test3()
    {
        // Simple socket server/client.  IPC verification
        ZSocketServer s = new ZSocketServer(ZMQ.REP,
                                          "ipc",
                                          "test3.ipc");
        ZSocketClient c = new ZSocketClient(ZMQ.REQ,
                                          "ipc",
                                          "test3.ipc");
        s.bind();
        c.connect();
        String tx_msg = "hello there...";
        c.send(tx_msg);
        String msg = s.recv();
        assert msg == tx_msg;
        c.close();
        s.close();
        System.out.println("test3() PASSED!\n");
    }

    public static void test4()
    {
        // Simple socket server/client.  PUSH/PULL verification.
        ZSocketServer s = new ZSocketServer(ZMQ.PULL,
                                          "ipc",
                                          "test3.ipc");
        ZSocketClient c = new ZSocketClient(ZMQ.PUSH,
                                          "ipc",
                                          "test3.ipc");
        s.bind();
        c.connect();
        String tx_msg = "hello there...";
        c.send(tx_msg);
        String msg = s.recv();
        assert msg == tx_msg;
        c.close();
        s.close();
        System.out.println("test4() PASSED!\n");
    }
}
