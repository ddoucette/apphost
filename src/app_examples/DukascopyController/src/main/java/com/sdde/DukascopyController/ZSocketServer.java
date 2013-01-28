
package com.sdde.DukascopyController;

import java.util.Collection;
import org.zeromq.*;


public class ZSocketServer extends ZSocket {

    private String bind_address;
    private int port_range[];
    private int port;
    private Llog log;

    public ZSocketServer (  int socket_type,
                            String protocol_name,
                            String bind_address,
                            int[] port_range )
    {
        super (socket_type);
        assert port_range.length > 0 && port_range.length <= 2;
        assert protocol_name == "tcp" || protocol_name == "ipc";

        for (int i = 0; i < port_range.length; i++)
        {
            assert port_range[i] > 0 && port_range[i] < 65535;
        }
        this.log = new Llog("ZSocketServer");

        this.bind_address = bind_address;
        this.port_range = port_range;
        this.protocol_name = protocol_name;
    }

    private void bind_tcp()
    {
        // Loop through each port in our port range and attempt
        // to bind the socket.
        int port = this.port_range[0];
        int port_stop = 0;

        if (this.port_range.length == 2)
            port_stop = this.port_range[1];
        else
            port_stop = port + 1;

        boolean bound = false;

        while (port < port_stop)
        {
            this.port = port;
            this.location = this.protocol_name
                                    + "://" + this.bind_address
                                    + ":" + this.port;
            try {
                this.socket.bind(this.location);
                this.log.debug("bound to location (" + this.location + ")");
                bound = true;
                break;
            } catch (Exception e) {
                // Failed to bind, try the next port
                this.log.debug("Failed to bind to location " + this.location);
                port += 1;
            }
        }
        assert bound == true;
    }

    private void bind_ipc()
    {
        this.location = this.protocol_name
                            + "://" + this.bind_address;
        this.socket.bind(this.location);
        this.log.debug("bound to IPC channel (" + this.location + ")");
    }

    public void bind()
    {
        this.create_socket();
        if (this.protocol_name == "tcp")
            this.bind_tcp();
        else if (this.protocol_name == "ipc")
            this.bind_ipc();
        else
            assert false;
    }
}
