package com.osinka.camel.beanstalk;

/**
 *
 * @author alaz
 */
public final class Headers {
    // in
    public static final String PRIORITY     = "beanstalk.priority";
    public static final String DELAY        = "beanstalk.delay";
    public static final String TIME_TO_RUN  = "beanstalk.timeToRun";

    // in/out
    public static final String JOB_ID       = "beanstalk.jobId";

    // out
    public static final String RESULT       = "beanstalk.result";
}