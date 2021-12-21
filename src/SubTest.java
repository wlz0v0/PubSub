import PubSubUtil.Subscriber;

/**
 * <pre>
 *     author : 武连增
 *     e-mail : wulianzeng@bupt.edu.cn
 *     time   : 2021/12/20
 *     desc   :
 *     version:
 * </pre>
 */
public class SubTest {
    public static void main(String[] args) {
        Subscriber<PubTest.MyMessage> subscriber = new Subscriber<>("123", 5, System.out::println);
        for (int i = 0; i < 10; ++i) {
            subscriber.subscribe();
        }
        Subscriber<Character> subscriber2 = new Subscriber<>("char", 5, System.out::println);
        for (int i = 0; i < 10; ++i) {
            subscriber2.subscribe();
        }
    }
}
