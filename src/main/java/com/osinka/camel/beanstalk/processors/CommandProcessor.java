package com.osinka.camel.beanstalk.processors;

import org.apache.camel.Processor;

public interface CommandProcessor extends Processor {
    public void init();
}