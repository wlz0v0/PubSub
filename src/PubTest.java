import PubSubUtil.Publisher;

import java.io.Serializable;

/**
 * <pre>
 *     author : 武连增
 *     e-mail : wulianzeng@bupt.edu.cn
 *     time   : 2021/12/20
 *     desc   :
 *     version:
 * </pre>
 */
public class PubTest {
    public static void main(String[] args) {
        Publisher<MyMessage> pub = new Publisher<>("123", 5);
        for (int i = 0; i < 10; ++i) {
            var msg = new MyMessage(i, (char) (i + '0'));
            System.out.println(msg);
            pub.publish(msg);
        }
        Publisher<Character> pub2 = new Publisher<>("char", 5);
        for (int i = 0; i < 10; ++i) {
            var msg = (char) (i + 'a');
            System.out.println();
            pub2.publish(msg);
        }
    }

    public static class MyMessage implements Serializable {
        int a;
        char b;

        public MyMessage(int a, char b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public String toString() {
            return "MyMessage{" +
                    "a=" + a +
                    ", b=" + b +
                    '}';
        }
    }
}
