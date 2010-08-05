package com.osinka.camel.beanstalk;

/**
 *
 * @author alaz
 */
public final class Headers {
    // in
    public static final String PRIORITY     = "camel.beanstalk.priority";
    public static final String DELAY        = "camel.beanstalk.delay";
    public static final String TIME_TO_RUN  = "camel.beanstalk.timeToRun";

    // in/out
    public static final String JOB_ID       = "camel.beanstalk.jobId";

    // out
    public static final String RESULT       = "camel.beanstalk.result";
}