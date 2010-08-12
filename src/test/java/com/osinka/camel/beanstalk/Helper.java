package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.camel.CamelContext;

/**
 *
 * @author alaz
 */
public final class Helper {
    public static ConnectionSettings mockConn(final Client client) {
        return new ConnectionSettings("tube") {
            @Override
            public Client newReadingClient() {
                return client;
            }
            
            @Override
            public Client newWritingClient() {
                return client;
            }
        };
    }

    public static BeanstalkEndpoint getEndpoint(String uri, CamelContext context, Client client) throws Exception {
        BeanstalkEndpoint endpoint = new BeanstalkEndpoint(uri, context.getComponent("beanstalk"), mockConn(client));
        context.addEndpoint(uri, endpoint);
        return endpoint;
    }

    public static byte[] stringToBytes(final String s) throws IOException {
        final ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
        final DataOutputStream dataStream = new DataOutputStream(byteOS);

        try {
            dataStream.writeBytes(s);
            dataStream.flush();
            return byteOS.toByteArray();
        } finally {
            dataStream.close();
            byteOS.close();
        }
    }
}
