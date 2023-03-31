package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 *线程池工具类
 */
public class ThreadPoolUtil {
    private  static ThreadPoolExecutor poolExecutor;

    //synchronized 修饰方法 和 代码块
    public static ThreadPoolExecutor getInstance(){
        if(poolExecutor==null){
            synchronized (ThreadPoolUtil.class){
                if(poolExecutor==null){
                    System.out.println("线程池对象");
                poolExecutor=new ThreadPoolExecutor(
                        4,
                        20,
                        60,
                        TimeUnit.SECONDS,
                        new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return poolExecutor;
    }


}
