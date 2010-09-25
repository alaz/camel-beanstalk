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

package com.osinka.camel.beanstalk;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.impl.DefaultProducer;
import com.osinka.camel.beanstalk.processors.CommandProcessor;
import com.osinka.camel.beanstalk.processors.ProcessExchangeTask;

/**
 *
 * @author <a href="mailto:azarov@osinka.com">Alexander Azarov</a>
 */
class SingleThreadedDelegatedProcessor extends DefaultProducer implements AsyncProcessor {
    final ExecutorService executorService;
    final CommandProcessor processor;

    public SingleThreadedDelegatedProcessor(BeanstalkEndpoint endpoint, final CommandProcessor processor) throws Exception {
        super(endpoint);
        this.executorService = endpoint.getCamelContext().getExecutorServiceStrategy().newSingleThreadExecutor(this, "Beanstalk");
        this.processor = processor;

    }

    @Override
    public void process(final Exchange exchange) throws Exception {
        Future f = executorService.submit(new ProcessExchangeTask(exchange, processor));
        f.get();
    }

    @Override
    public boolean process(final Exchange exchange, final AsyncCallback callback) {
        executorService.submit(new ProcessExchangeTask(exchange, processor, callback));
        return false;
    }

    @Override
    public void doStart() {
        // The first task is to init CommandProcessor.
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                processor.init();
            }
        });
    }

    @Override
    public void doStop() {
        executorService.shutdown();
    }
}