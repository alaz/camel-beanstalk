package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultProducer;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class BeanstalkProducer extends DefaultProducer {
    private final transient Log log = LogFactory.getLog(BeanstalkProducer.class);
    final Client beanstalk;

    BeanstalkProducer(BeanstalkEndpoint endpoint, Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(Exchange exchange) {
        Message in = exchange.getIn();

        Long priority = in.getHeader(Headers.PRIORITY, Long.valueOf(BeanstalkComponent.DEFAULT_PRIORITY), Long.class);
        Integer delay = in.getHeader(Headers.DELAY, Integer.valueOf(BeanstalkComponent.DEFAULT_DELAY), Integer.class);
        Integer timeToRun = in.getHeader(Headers.TIME_TO_RUN, Integer.valueOf(BeanstalkComponent.DEFAULT_TIME_TO_RUN), Integer.class);

        long jobId = beanstalk.put(priority.longValue(), delay.intValue(), timeToRun.intValue(), in.getBody(byte[].class));
        log.debug(String.format("Task with priority=%l, delay=%d seconds, time to run %d put with jobId %d", priority, delay, timeToRun, jobId));

        Message answer = in;
        if (ExchangeHelper.isOutCapable(exchange)) {
            answer = exchange.getOut();
            // preserve headers
            answer.getHeaders().putAll(in.getHeaders());
        }

        answer.setHeader(Headers.JOB_ID, Long.valueOf(jobId));
    }
}
