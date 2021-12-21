package PubSubUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * 工具类，{@link Publisher}和{@link Subscriber}共用的方法
 *
 * @author 武连增
 * @since 11
 */
public class BufferUtil {
    /**
     * 管理器端口号
     */
    final static int CORE_PORT = 9999;

    /**
     * 工具类不可实例化
     */
    private BufferUtil() {
        throw new AssertionError();
    }

    /**
     * 获取Buffer的端口号以使用Socket连接
     *
     * @param topic    话题名
     * @param capacity 消息队列容量
     * @return Buffer的端口号
     */
    static int receivePortOfTopic(String topic, int capacity) {
        int port = -1;
        try (var socket = new Socket("localhost", CORE_PORT);
             var oos = new ObjectOutputStream(socket.getOutputStream());
             var ois = new ObjectInputStream(socket.getInputStream())) {
            oos.writeObject(topic);
            oos.writeInt(capacity);
            oos.flush();
            port = ois.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return port;
    }
}
