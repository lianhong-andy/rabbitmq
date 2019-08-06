package com.andy.thread;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * @author lianhong
 * @description 实现Runnable方式
 * @date 2019/8/4 0004下午 4:25
 */
public class FutureTaskWay {
    @Test
    public void testThread(){
        FutureTask<String> futureTask = new FutureTask<>(new CallTask());
        new Thread(futureTask).start();

        try {
            //等待任务执行完毕并返回执行结果
            String result = futureTask.get();
            System.out.println("result = " + result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static class CallTask implements Callable<String> {

        @Override
        public String call() throws Exception {
            System.out.println(" create thread by imple callable ..." );
            return "hello callable";
        }
    }

}
