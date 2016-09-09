package com.qunar.flight.dubbo.duck.util;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * since 2016/9/7.
 */
public class MatchDuck {

    private static final Logger log = LoggerFactory.getLogger(MatchDuck.class);

    /**
     * consumer://10.86.144.71/com.qunar.jt.demo.hello.api.HelloService?application=hello-consumer&category=providers,configurators,routers&dubbo=2.5.3&group=hello2&interface=com.qunar.jt.demo.hello.api.HelloService&logger=slf4j&methods=hello&organization=ze&pid=7120&side=consumer&timestamp=1473409537344
     * dubbo://10.86.144.71:20880/com.qunar.jt.demo.hello.api.HelloService?anyhost=true&application=hello-provider&dubbo=2.5.3&group=hello1&interface=com.qunar.jt.demo.hello.api
     *
     * @param consumerUrl
     * @param providerUrl
     * @return
     */
    public static boolean isMatch(URL consumerUrl, URL providerUrl) {
        log.warn("duck match url consumer {} provider {}", consumerUrl, providerUrl);
        String orgKey = "organization";
        String consumerOrg = consumerUrl.getParameter(orgKey);
        String providerOrg = providerUrl.getParameter(orgKey);
        if (StringUtils.isBlank(consumerOrg) || StringUtils.isBlank(providerOrg)) {
            log.info("no org, not duck match");
            return false;
        }

        String consumerInterface = consumerUrl.getServiceInterface();
        String providerInterface = providerUrl.getServiceInterface();
        if( ! (Constants.ANY_VALUE.equals(consumerInterface) || StringUtils.isEquals(consumerInterface, providerInterface)) ) return false;

        if (! UrlUtils.isMatchCategory(providerUrl.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY),
                consumerUrl.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY))) {
            return false;
        }
        if (! providerUrl.getParameter(Constants.ENABLED_KEY, true)
                && ! Constants.ANY_VALUE.equals(consumerUrl.getParameter(Constants.ENABLED_KEY))) {
            return false;
        }

        String consumerGroup = consumerUrl.getParameter(Constants.GROUP_KEY);
        String consumerVersion = consumerUrl.getParameter(Constants.VERSION_KEY);
        String consumerClassifier = consumerUrl.getParameter(Constants.CLASSIFIER_KEY, Constants.ANY_VALUE);
        //不比较group和version
        String providerClassifier = providerUrl.getParameter(Constants.CLASSIFIER_KEY, Constants.ANY_VALUE);
        return (consumerClassifier == null || Constants.ANY_VALUE.equals(consumerClassifier) || StringUtils.isEquals(consumerClassifier, providerClassifier))
                && (consumerOrg.equals(providerOrg))
                ;

    }
}
