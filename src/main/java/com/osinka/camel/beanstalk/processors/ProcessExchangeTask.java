package com.osinka.camel.beanstalk.processors;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class ProcessExchangeTask implements Runnable {
    private final Exchange exchange;
    private final Processor processor;
    private final AsyncCallback callback;

    public ProcessExchangeTask(Exchange exchange, Processor processor, AsyncCallback callback) {
        this.exchange = exchange;
        this.processor = processor;
        this.callback = callback;
    }

    public ProcessExchangeTask(Exchange exchange, Processor processor) {
        this(exchange, processor, null);
    }

    @Override
    public void run() {
        try {
            processor.process(exchange);
        } catch (Throwable t) {
            exchange.setException(t);
        } finally {
            if (callback != null)
                callback.done(false);
        }
    }

}
