package PubSubUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

/**
 * <p>
 * 用于从话题接收消息的Subscriber，每个话题有自己的消息队列{@link Buffer}，
 * 当消息队列为空时接收消息会阻塞直到消息队列不为空。<br>
 * 请注意：<br>
 * 1.同一个话题的Publisher和Subscriber的消息类型必须一致；<br>
 * 2.运行Subscriber前必须运行Core程序{@link BufferManager#launch()}。
 * </p>
 *
 * @param <E> 消息队列的消息类型，必须实现{@link Serializable}接口
 * @author 武连增
 * @since 11
 */
@SuppressWarnings("unused")
public class Subscriber<E extends Serializable> {
    /**
     * Subscriber的话题名
     */
    private final String topic;

    /**
     * Buffer中ServerSocket的端口
     */
    private final int port;

    /**
     * 处理消息的回调接口
     */
    private final MessageHandler<E> callback;

    /**
     * @param topic    Subscriber要subscribe的话题名
     * @param capacity 消息队列容量
     * @param handler  消息处理的回调函数
     *
     * @throws NullPointerException 当topic或handler为null时
     */
    public Subscriber(String topic, int capacity, MessageHandler<E> handler) {
        if (topic == null || handler == null) {
            throw new NullPointerException();
        }
        this.topic = topic;
        this.port = BufferUtil.receivePortOfTopic(topic, capacity);
        this.callback = handler;
    }

    /**
     * 从消息队列中取出一条消息并在回调函数中处理，当消息队列为空时接收消息会阻塞直到消息队列不为空
     */
    public void subscribe() {
        try (var socket = new Socket("localhost", port);
             var oos = new ObjectOutputStream(socket.getOutputStream());
             var ois = new ObjectInputStream(socket.getInputStream())) {
            // 表明不停止
            oos.writeObject("ok");
            // 表明自己是一个Subscriber
            oos.writeBoolean(false);
            // 通知Buffer要读取数据了
            oos.writeBoolean(true);
            oos.flush();
            @SuppressWarnings("unchecked")
            var msg = (E) ois.readObject();
            callback.handle(msg);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return Subscriber的话题名
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 消息处理接口，定义了一个回调函数来处理得到的消息
     *
     * @param <E> 消息类型
     */
    public interface MessageHandler<E> {
        /**
         * 消息处理回调函数，处理接收到的消息
         *
         * @param msg 接收到的消息
         */
        void handle(E msg);
    }
}
