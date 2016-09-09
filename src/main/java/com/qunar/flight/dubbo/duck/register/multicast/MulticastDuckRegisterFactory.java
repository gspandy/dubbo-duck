package com.qunar.flight.dubbo.duck.register.multicast;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.support.AbstractRegistryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * since 2016/9/5.
 */
public class MulticastDuckRegisterFactory extends AbstractRegistryFactory {

    private static final Logger log = LoggerFactory.getLogger(MulticastDuckRegisterFactory.class);

    @Override
    protected Registry createRegistry(URL url) {
        log.info("new MutilcastDuckRegister {}", url);
        return new MutilcastDuckRegister(url);
    }
}
