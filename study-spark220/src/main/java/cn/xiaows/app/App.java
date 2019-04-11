package cn.xiaows.app;

/**
 * TODO
 *
 * @author: xiaows
 * @create: 2019-01-22 14:39
 * @version: v1.0
 */
public class App {
    public static void main(String[] args) {
        Base base = new Son1();
        System.out.println("=========");
        Son1 son1 = new Son1();
        System.out.println("=========");
        base.setStr(String.valueOf(1));
        System.out.println(base.str);
        System.out.println(son1.str);
    }

}
