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

package com.osinka.camel.beanstalk.integration;

import com.osinka.camel.beanstalk.ConnectionSettings;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;

public abstract class BeanstalkCamelTestSupport extends CamelTestSupport {
    protected ThreadLocal<Client> client = null;

    protected Client beanstalk() {
        return client.get();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        final String tube = getTubeName();
        client = new ConnectionSettings(tube).newWritingClient();
        beanstalk().watch(tube);
        clearTube();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        clearTube();
        super.tearDown();
    }

    protected void clearTube() {
        if (beanstalk() != null) {
            Job job;
            while (beanstalk().kick(100) >0)
                ;
            while ((job = beanstalk().reserve(0)) != null)
                beanstalk().delete(job.getJobId());
        }
    }

    protected abstract String getTubeName();
}
