package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;

/**
 *
 * @author alaz
 */
public class ConnectionSettings {
    final String host;
    final int port;
    final String tube;

    public ConnectionSettings(String host, int port, String tube) {
        this.host = host;
        this.port = port;
        this.tube = tube;
    }

    public ConnectionSettings(String host, String tube) {
        this(host, Client.DEFAULT_PORT, tube);
    }

    public ConnectionSettings(String tube) {
        this(Client.DEFAULT_HOST, tube);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConnectionSettings) {
            ConnectionSettings other = (ConnectionSettings) obj;
            return other.host.equals(host) && other.port == port && other.tube.equals(tube);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 41*(41*(41+host.hashCode())+port)+tube.hashCode();
    }

    @Override
    public String toString() {
        return "beanstalk://"+host+":"+port+"/"+tube;
    }
}