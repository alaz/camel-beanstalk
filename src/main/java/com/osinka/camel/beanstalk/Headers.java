package com.osinka.camel.beanstalk;

import org.apache.camel.Message;

/**
 *
 * @author alaz
 */
public final class Headers {
    public static final String PRIORITY    = "camel.beanstalk.priority";
    public static final String DELAY       = "camel.beanstalk.delay";
    public static final String TIME_TO_RUN = "camel.beanstalk.timeToRun";
    public static final String JOB_ID      = "camel.beanstalk.jobId";
}