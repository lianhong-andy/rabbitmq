package com.andy.thread;

import org.junit.Test;

/**
 * @author lianhong
 * @description 继承thread方式
 * @date 2019/8/4 0004下午 4:25
 */
public class ExtendThread {
    @Test
    public void testThread(){
        /**
         * 调用start的方法之后并线程并没有马上执行，而是出于就绪状态，这个就绪状态是指该线程已经获取了除cpu资源外的其它资源，
         * 等待cpu资源后才会真正出于运转状态，一旦run方法执行完毕，该线程就会出于终止状态。
         */
        new MyThread().start();
    }

    public static class MyThread extends Thread {
        @Override
        public void run() {
            System.out.println("create thread by extending Thread..." );
        }
    }


}
