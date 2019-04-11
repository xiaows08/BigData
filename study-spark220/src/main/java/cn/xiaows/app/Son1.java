package cn.xiaows.app;

/**
 * TODO
 *
 * @author: xiaows
 * @create: 2019-01-22 14:38
 * @version: v1.0
 */
public class Son1 extends Base {
    @Override
    public void setStr(String str) {
       super.str = str;
    }

    public Son1() {
        System.out.println("son1 init...");
    }
}
