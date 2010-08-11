package com.osinka.camel.beanstalk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 *
 * @author alaz
 */
public final class Helper {
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
