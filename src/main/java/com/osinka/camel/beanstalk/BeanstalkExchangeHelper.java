package com.osinka.camel.beanstalk;

import org.apache.camel.Message;

/**
 *
 * @author alaz
 */
public final class BeanstalkExchangeHelper {
    public static long getPriority(final BeanstalkEndpoint endpoint, final Message in) {
        return in.getHeader(Headers.PRIORITY, Long.valueOf(endpoint.getJobPriority()), Long.class).longValue();
    }

    public static int getDelay(final BeanstalkEndpoint endpoint, final Message in) {
        return in.getHeader(Headers.DELAY, Integer.valueOf(endpoint.getJobDelay()), Integer.class).intValue();
    }

    public static int getTimeToRun(final BeanstalkEndpoint endpoint, final Message in) {
        return in.getHeader(Headers.TIME_TO_RUN, Integer.valueOf(endpoint.getJobTimeToRun()), Integer.class).intValue();
    }
}
