package com.osinka.camel.beanstalk;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultProducer;
import org.apache.camel.util.ExchangeHelper;

/**
 *
 * @author alaz
 */
public abstract class AbstractBeanstalkProducer extends DefaultProducer {
    public AbstractBeanstalkProducer(BeanstalkEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }

    public long getPriority(final Message in) {
        return in.getHeader(Headers.PRIORITY, Long.valueOf(getEndpoint().getJobPriority()), Long.class).longValue();
    }

    public int getDelay(final Message in) {
        return in.getHeader(Headers.DELAY, Integer.valueOf(getEndpoint().getJobDelay()), Integer.class).intValue();
    }

    public int getTimeToRun(final Message in) {
        return in.getHeader(Headers.TIME_TO_RUN, Integer.valueOf(getEndpoint().getJobTimeToRun()), Integer.class).intValue();
    }

    public Message getAnswerMessage(final Exchange exchange) {
        Message answer = exchange.getIn();
        if (ExchangeHelper.isOutCapable(exchange)) {
            answer = exchange.getOut();
            // preserve headers
            answer.getHeaders().putAll(exchange.getIn().getHeaders());
        }
        return answer;
    }

    public void answerWith(final Exchange exchange, final String header, final Object value) {
        final Message answer = getAnswerMessage(exchange);
        answer.setHeader(header, value);
    }
}
