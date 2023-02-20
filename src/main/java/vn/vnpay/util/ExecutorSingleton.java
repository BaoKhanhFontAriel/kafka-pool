package vn.vnpay.util;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Slf4j
@Getter
@Setter
@ToString
public class ExecutorSingleton {
    private static ExecutorSingleton instance;
    private ExecutorService executorService;

    public ExecutorSingleton(){
        log.info("create new ExecutorServiceSingleton...");
        this.executorService = Executors.newFixedThreadPool(1000) ;
    }

    public static ExecutorSingleton getInstance(){
        if(instance == null){
            instance = new ExecutorSingleton();
        }
        return instance;
    }

    public static void shutdownNow() {
        instance.executorService.shutdownNow();
    }

    public static Future submit(Runnable runnable) {
        return instance.executorService.submit(runnable);
    }

    public static Future submit(Callable<String> callable) {
        return instance.executorService.submit(callable);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }
}
