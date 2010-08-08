/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.osinka.camel.beanstalk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class Helper {
    public static byte[] stringToBytes(final String s) throws IOException {
        final ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
        final DataOutputStream dataStream = new DataOutputStream(byteOS);

        try {
            dataStream.writeUTF(s);
            dataStream.flush();
            return byteOS.toByteArray();
        } finally {
            dataStream.close();
            byteOS.close();
        }
    }
}
