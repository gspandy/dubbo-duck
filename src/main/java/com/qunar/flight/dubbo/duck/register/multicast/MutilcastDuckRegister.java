package com.qunar.flight.dubbo.duck.register.multicast;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.qunar.flight.dubbo.duck.util.MatchDuck;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * since 2016/9/5.
 */
public class MutilcastDuckRegister extends FailbackRegistry {

    private static final int DEFAULT_MULTICAST_PORT = 1234;

    private final InetAddress mutilcastAddress;

    private final MulticastSocket mutilcastSocket;

    private final int mutilcastPort;


    private final ConcurrentMap<URL, Set<URL>> received = new ConcurrentHashMap<URL, Set<URL>>();


    public MutilcastDuckRegister(URL url) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        if (!isMulticastAddress(url.getHost())) {
            throw new IllegalArgumentException("Invalid multicast address " + url.getHost() + ", " +
                    "scope: 224.0.0.0 - 239.255.255.255");
        }
        try {
            mutilcastAddress = InetAddress.getByName(url.getHost());
            mutilcastPort = url.getPort() <= 0 ? DEFAULT_MULTICAST_PORT : url.getPort();
            mutilcastSocket = new MulticastSocket(mutilcastPort);
            mutilcastSocket.setLoopbackMode(false);
            mutilcastSocket.joinGroup(mutilcastAddress);
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    byte[] buf = new byte[2048];
                    DatagramPacket recv = new DatagramPacket(buf, buf.length);
                    while (!mutilcastSocket.isClosed()) {
                        try {
                            mutilcastSocket.receive(recv);
                            String msg = new String(recv.getData()).trim();
                            int i = msg.indexOf('\n');
                            if (i > 0) {
                                msg = msg.substring(0, i).trim();
                            }
                            MutilcastDuckRegister.this.receive(msg, (InetSocketAddress) recv.getSocketAddress());
                            Arrays.fill(buf, (byte) 0);
                        } catch (Throwable e) {
                            if (!mutilcastSocket.isClosed()) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }
                }
            }, "DubboMulticastRegistryReceiver");
            thread.setDaemon(true);
            thread.start();
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static boolean isMulticastAddress(String ip) {
        int i = ip.indexOf('.');
        if (i > 0) {
            String prefix = ip.substring(0, i);
            if (StringUtils.isInteger(prefix)) {
                int p = Integer.parseInt(prefix);
                return p >= 224 && p <= 239;
            }
        }
        return false;
    }

    private void receive(String msg, InetSocketAddress remoteAddress) {
        if (logger.isInfoEnabled()) {
            logger.info("Receive multicast message: " + msg + " from " + remoteAddress);
        }
        if (msg.startsWith(Constants.REGISTER)) {
            URL url = URL.valueOf(msg.substring(Constants.REGISTER.length()).trim());
            registered(url);
        } else if (msg.startsWith(Constants.UNREGISTER)) {
            URL url = URL.valueOf(msg.substring(Constants.UNREGISTER.length()).trim());
            unregistered(url);
        } else if (msg.startsWith(Constants.SUBSCRIBE)) {
            URL url = URL.valueOf(msg.substring(Constants.SUBSCRIBE.length()).trim());
            Set<URL> urls = getRegistered();
            if (urls != null && urls.size() > 0) {
                for (URL u : urls) {
                    if (UrlUtils.isMatch(url, u) || MatchDuck.isMatch(url, u)) {
                        String host = remoteAddress != null && remoteAddress.getAddress() != null
                                ? remoteAddress.getAddress().getHostAddress() : url.getIp();
                        if (url.getParameter("unicast", true) // 消费者的机器是否只有一个进程
                                && !NetUtils.getLocalHost().equals(host)) { // 同机器多进程不能用unicast单播信息，否则只会有一个进程收到信息
                            unicast(Constants.REGISTER + " " + u.toFullString(), host);
                        } else {
                            broadcast(Constants.REGISTER + " " + u.toFullString());
                        }
                    }
                }
            }
        }/* else if (msg.startsWith(UNSUBSCRIBE)) {
        }*/
    }

    protected void registered(URL url) {
        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL key = entry.getKey();
            if (UrlUtils.isMatch(key, url) || MatchDuck.isMatch(key, url)) {
                Set<URL> urls = received.get(key);
                if (urls == null) {
                    received.putIfAbsent(key, new ConcurrentHashSet<URL>());
                    urls = received.get(key);
                }
                urls.add(url);
                List<URL> list = toList(urls);
                for (NotifyListener listener : entry.getValue()) {
                    notify(key, listener, list);
                    synchronized (listener) {
                        listener.notify();
                    }
                }
            }
        }
    }

    private List<URL> toList(Set<URL> urls) {
        List<URL> list = new ArrayList<URL>();
        if (urls != null && urls.size() > 0) {
            for (URL url : urls) {
                list.add(url);
            }
        }
        return list;
    }

    private void unicast(String msg, String host) {
        if (logger.isInfoEnabled()) {
            logger.info("Send unicast message: " + msg + " to " + host + ":" + mutilcastPort);
        }
        try {
            byte[] data = (msg + "\n").getBytes();
            DatagramPacket hi = new DatagramPacket(data, data.length, InetAddress.getByName(host), mutilcastPort);
            mutilcastSocket.send(hi);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void broadcast(String msg) {
        if (logger.isInfoEnabled()) {
            logger.info("Send broadcast message: " + msg + " to " + mutilcastAddress + ":" + mutilcastPort);
        }
        try {
            byte[] data = (msg + "\n").getBytes();
            DatagramPacket hi = new DatagramPacket(data, data.length, mutilcastAddress, mutilcastPort);
            mutilcastSocket.send(hi);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    protected void unregistered(URL url) {
        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL key = entry.getKey();
            if (UrlUtils.isMatch(key, url)) {
                Set<URL> urls = received.get(key);
                if (urls != null) {
                    urls.remove(url);
                }
                List<URL> list = toList(urls);
                for (NotifyListener listener : entry.getValue()) {
                    notify(key, listener, list);
                }
            }
        }
    }


    @Override
    protected void doRegister(URL url) {
        broadcast(Constants.REGISTER + " " + url.toFullString());
    }

    @Override
    protected void doUnregister(URL url) {
        broadcast(Constants.UNREGISTER + " " + url.toFullString());
    }

    @Override
    protected void doSubscribe(URL url, NotifyListener listener) {
        //if (Constants.ANY_VALUE.equals(url.getServiceInterface())) {
        //          admin = true;
        //      }
        broadcast(Constants.SUBSCRIBE + " " + url.toFullString());
        synchronized (listener) {
            try {
                listener.wait(url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT));
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    protected void doUnsubscribe(URL url, NotifyListener listener) {
        if (!Constants.ANY_VALUE.equals(url.getServiceInterface())
                && url.getParameter(Constants.REGISTER_KEY, true)) {
            unregister(url);
        }
        broadcast(Constants.UNSUBSCRIBE + " " + url.toFullString());
    }

    @Override
    public boolean isAvailable() {
        try {
            return mutilcastSocket != null;
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            //if (cleanFuture != null) {
            //    cleanFuture.cancel(true);
            //}
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        try {
            mutilcastSocket.leaveGroup(mutilcastAddress);
            mutilcastSocket.close();
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    @Override
    public void register(URL url) {
        super.register(url);
        registered(url);
    }

    @Override
    public void unregister(URL url) {
        super.unregister(url);
        unregistered(url);
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        super.subscribe(url, listener);
        subscribed(url, listener);
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        super.unsubscribe(url, listener);
        received.remove(url);
    }

    @Override
    public List<URL> lookup(URL url) {
        List<URL> urls = new ArrayList<URL>();
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        if (notifiedUrls != null && notifiedUrls.size() > 0) {
            for (List<URL> values : notifiedUrls.values()) {
                urls.addAll(values);
            }
        }
        if (urls == null || urls.size() == 0) {
            List<URL> cacheUrls = getCacheUrls(url);
            if (cacheUrls != null && cacheUrls.size() > 0) {
                urls.addAll(cacheUrls);
            }
        }
        if (urls == null || urls.size() == 0) {
            for (URL u : getRegistered()) {
                if (UrlUtils.isMatch(url, u) || MatchDuck.isMatch(url, u)) {
                    urls.add(u);
                }
            }
        }
        if (Constants.ANY_VALUE.equals(url.getServiceInterface())) {
            for (URL u : getSubscribed().keySet()) {
                if (UrlUtils.isMatch(url, u)) {
                    urls.add(u);
                }
            }
        }
        logger.info("look up urls " + urls);
        return urls;
    }


    protected void subscribed(URL url, NotifyListener listener) {
        List<URL> urls = lookup(url);
        notify(url, listener, urls);
    }

    @Override
    protected void doNotify(URL url, NotifyListener listener, List<URL> urls) {
        logger.warn("self do notify");
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((urls == null || urls.size() == 0)
                && !Constants.ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", urls: " + urls);
        }
        Map<String, List<URL>> result = new HashMap<String, List<URL>>();
        for (URL u : urls) {
            if (UrlUtils.isMatch(url, u) || MatchDuck.isMatch(url, u)) {
                String category = u.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
                List<URL> categoryList = result.get(category);
                if (categoryList == null) {
                    categoryList = new ArrayList<URL>();
                    result.put(category, categoryList);
                }
                categoryList.add(u);
            }
        }
        if (result.size() == 0) {
            return;
        }
        Map<String, List<URL>> categoryNotified = getNotified().get(url);
        if (categoryNotified == null) {
            ((ConcurrentHashMap<URL, Map<String, List<URL>>>) getNotified()).putIfAbsent(url, new ConcurrentHashMap<String, List<URL>>());
            categoryNotified = getNotified().get(url);
        }
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            categoryNotified.put(category, categoryList);
            //saveProperties(url);
            listener.notify(categoryList);
        }
    }

/*
    @Override
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((urls == null || urls.size() == 0)
                && ! Constants.ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", urls: " + urls);
        }
        Map<String, List<URL>> result = new HashMap<String, List<URL>>();
        for (URL u : urls) {
            if (UrlUtils.isMatch(url, u) || MatchDuck.isMatch(url, u)) {
            	String category = u.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
            	List<URL> categoryList = result.get(category);
            	if (categoryList == null) {
            		categoryList = new ArrayList<URL>();
            		result.put(category, categoryList);
            	}
            	categoryList.add(u);
            }
        }
        if (result.size() == 0) {
            return;
        }
        Map<String, List<URL>> categoryNotified = getNotified().get(url);
        if (categoryNotified == null) {
            ((ConcurrentHashMap<URL, Map<String, List<URL>>>)getNotified()).putIfAbsent(url,
            new ConcurrentHashMap<String, List<URL>>());
            categoryNotified = getNotified().get(url);
        }
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            categoryNotified.put(category, categoryList);
            listener.notify(categoryList);
        }
    }
    */

}
