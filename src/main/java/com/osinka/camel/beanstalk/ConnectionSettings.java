package com.osinka.camel.beanstalk;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Scanner;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClientImpl.ClientImpl;

/**
 *
 * @author alaz
 */
public class ConnectionSettings {
    final String host;
    final int port;
    final String[] tubes;

    public ConnectionSettings(final String tube) {
        this(Client.DEFAULT_HOST, Client.DEFAULT_PORT, tube);
    }

    public ConnectionSettings(final String host, final String tube) {
        this(host, Client.DEFAULT_PORT, tube);
    }

    public ConnectionSettings(final String host, final int port, final String tube) {
        this.host = host;
        this.port = port;

        final Scanner scanner = new Scanner(tube);
        scanner.useDelimiter("\\+");
        final ArrayList<String> buffer = new ArrayList<String>();
        while (scanner.hasNext())
            buffer.add(scanner.next());
        this.tubes = buffer.toArray(new String[0]);
        scanner.close();
    }

    public Client newWritingClient() throws IllegalArgumentException {
        if (tubes.length > 1) {
            throw new IllegalArgumentException("There must be only one tube specified for Beanstalk producer");
        }

        final String tube = tubes.length > 0 ? tubes[0] : BeanstalkComponent.DEFAULT_TUBE;

        final ClientImpl client = new ClientImpl(host, port);
        client.useTube(tube);
        return client;
    }

    public Client newReadingClient() {
        final ClientImpl client = new ClientImpl(host, port);
        for (String tube : tubes)
            client.watch(tube);
        return client;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ConnectionSettings) {
            final ConnectionSettings other = (ConnectionSettings) obj;
            return other.host.equals(host) && other.port == port && Arrays.equals(other.tubes, tubes);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 41*(41*(41+host.hashCode())+port)+Arrays.hashCode(tubes);
    }

    @Override
    public String toString() {
        return "beanstalk://"+host+":"+port+"/"+Arrays.toString(tubes);
    }
}