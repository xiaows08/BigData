package cn.xiaows.app;

/**
 * TODO
 *
 * @author: xiaows
 * @create: 2019-01-22 14:38
 * @version: v1.0
 */
public abstract class Base {
    public String str;

    public abstract void setStr(String str);

    public Base() {
        System.out.println("Base init...");
    }
}
