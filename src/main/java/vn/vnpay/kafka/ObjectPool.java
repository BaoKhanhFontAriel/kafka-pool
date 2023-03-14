package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Set;

@Getter
@Setter
@Slf4j
public abstract class ObjectPool<T> {
    private final HashMap<T, Long> inUse;
    private final HashMap<T, Long> available;
    private long expirationTime = 30000;
    private int maxPoolSize = 10;

    protected ObjectPool() {
        inUse = new HashMap<>();
        available = new HashMap<>();
    }
    public synchronized void shutdown(){
        available.forEach((t, aLong) -> close(t));
        inUse.forEach((t, aLong) -> close(t));
        available.clear();
        inUse.clear();
    }

    protected abstract T create();
    protected abstract boolean isOpen(T o);
    protected abstract void close(T o);


    protected synchronized T getMember() throws InterruptedException {
        T t;
        // pool size is equal to max pool size, wait until available
        while (inUse.size() == maxPoolSize){
            log.info("Full inUse pool, waiting for available");
            wait();
        }

        long now = System.currentTimeMillis();
        if (available.size() > 0) {
            Set<T> e = available.keySet();
            while (!e.isEmpty()) {
                t = e.iterator().next();
                if ((now - available.get(t)) > expirationTime) {
                    // object has expired
                    available.remove(t);
                    close(t);
                } else {
                    if (isOpen(t)) {
                        available.remove(t);
                        inUse.put(t, now);
                        return (t);
                    } else {
                        available.remove(t);
                        close(t);
                    }
                }
            }
        }

        // no objects available, create a new one
        t = create();
        inUse.put(t, now);
        return (t);
    }

    protected synchronized void release(T t) {
        inUse.remove(t);
        available.put(t, System.currentTimeMillis());
    }
}
