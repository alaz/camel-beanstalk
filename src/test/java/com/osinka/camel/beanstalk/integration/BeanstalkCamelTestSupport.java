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
    protected Client client = null;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        final String tube = getTubeName();
        client = new ConnectionSettings(tube).newWritingClient();
        client.watch(tube);
        clearTube();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        clearTube();
        super.tearDown();
    }

    protected void clearTube() {
        if (client != null) {
            Job job;
            while (client.kick(100) >0)
                ;
            while ((job = client.reserve(0)) != null)
                client.delete(job.getJobId());
        }
    }

    protected abstract String getTubeName();
}
