package nio;

import java.util.concurrent.*;

public class Singleton {
    private static Singleton singleton;
    private Singleton(){

    }

    public static synchronized Singleton init(){
        if (singleton==null){
           synchronized (Singleton.class){
               if (singleton==null){
                   singleton=new Singleton();
               }
           }
        }
        return singleton;
    }
}

class MyThread extends Thread{
    public void run() {
        for(int i=0;i<100;i++){
            System.out.println("当前线程名为："+Thread.currentThread().getName());
        }
    }
}


class MyRunnable implements Runnable{
    public void run() {
        for(int i=0;i<100;i++){
            System.out.println("当前线程名为："+Thread.currentThread().getName());
        }
    }

    public static void main(String[] args) {
        ExecutorService exec = Executors.newCachedThreadPool();
        Executors.newFixedThreadPool(2);
        Executors.newScheduledThreadPool(2);
        Executors.newSingleThreadExecutor();
    }
}