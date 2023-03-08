package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@Setter
@Slf4j
public abstract class ObjectPool<T> {
    private final Hashtable<T, Long> inUse;
    private final Hashtable<T, Long> available;
    private long expirationTime;
    private int maxPoolSize;

    public ObjectPool() {
        maxPoolSize = 10;
        expirationTime = 30000;
        inUse = new Hashtable<>();
        available = new Hashtable<>();
    }
    public synchronized void shutdown(){
        available.forEach((t, aLong) -> expire(t));
        inUse.forEach((t, aLong) -> expire(t));
        available.clear();
        inUse.clear();
    }
    protected abstract T create();
    public abstract boolean validate(T o);
    public abstract void expire(T o);
    public synchronized T checkOut() throws InterruptedException {
        long now = System.currentTimeMillis();
        T t;
        // no objects available and pool size is bigger than max pool size, wait until available
//        while (inUse.size() == maxPoolSize){
//            log.info("inUse pool size is full, waiting for available");
//            wait();
//        }

        if (available.size() > 0) {
            Enumeration<T> e = available.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if ((now - available.get(t)) > expirationTime) {
                    // object has expired
                    available.remove(t);
                    expire(t);
                } else {
                    if (validate(t)) {
                        available.remove(t);
                        inUse.put(t, now);
                        log.info("move object from available to inuse");
                        return (t);
                    } else {
                        // object failed validation
                        available.remove(t);
                        expire(t);
                    }
                }
            }
        }

        // no objects available, create a new one
        t = create();
        inUse.put(t, now);
        return (t);
    }
    public synchronized void checkIn(T t) {
        inUse.remove(t);
        available.put(t, System.currentTimeMillis());
    }
}
