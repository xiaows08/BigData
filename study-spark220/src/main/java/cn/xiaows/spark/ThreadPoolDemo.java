package cn.xiaows.spark;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolDemo {
    public static void main(String[] args) {
        // 单线程的线程池
        // ExecutorService pool = Executors.newSingleThreadExecutor();
        // 固定数量线程的线程池
        // ExecutorService pool = Executors.newFixedThreadPool(5);
        // 可缓冲的线程池（可以有多个线程）
        ExecutorService pool = Executors.newCachedThreadPool();


        for (int i = 0; i < 20; i++) {
            // pool.submit(() -> {
            //     System.out.println();
            // }, "123");
            pool.execute(() -> {
                System.out.println(Thread.currentThread().getName());
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + " is over...");
            });
        }
        System.out.println("all threads are over!");
        // pool.shutdownNow();
        pool.shutdown();
    }
}
