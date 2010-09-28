/**
 * Copyright (C) 2010 Alexander Azarov <azarov@osinka.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
