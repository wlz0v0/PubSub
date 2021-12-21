package PubSubUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

/**
 * <p>
 * 用于向话题发布消息的Publisher，每个话题有自己的消息队列{@link Buffer}，
 * 当消息队列已满时发布消息会阻塞直到消息队列不为满。<br>
 * 请注意：<br>
 * 1.同一个话题的Publisher和{@link Subscriber}的消息类型必须一致；<br>
 * 2.运行Publisher前必须运行Core程序{@link BufferManager#launch()}。
 * </p>
 *
 * @param <E> 消息队列的消息类型，必须实现{@link Serializable}接口
 * @author 武连增
 * @since 11
 */
@SuppressWarnings("unused")
public class Publisher<E extends Serializable> {
    /**
     * Publisher的话题名
     */
    private final String topic;

    /**
     * Buffer中ServerSocket的端口
     */
    private final int port;

    /**
     * @param topic    Publisher要publish的话题名
     * @param capacity 消息队列容量
     * @throws NullPointerException 当topic为null时
     */
    public Publisher(String topic, int capacity) {
        if (topic == null) {
            throw new NullPointerException();
        }
        this.topic = topic;
        this.port = BufferUtil.receivePortOfTopic(topic, capacity);
    }

    /**
     * @return Publisher的话题名
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 向消息队列发送一条消息，当消息队列已满则会阻塞直到消息队列不为满
     *
     * @param msg 被发布的消息
     * @throws NullPointerException 当msg为null时
     */
    public void publish(E msg) {
        if (msg == null) throw new NullPointerException();
        // 由于不知道会发送多少次，于是在每次publish的时候都建立一次连接
        try (var socket = new Socket("localhost", port);
             var oos = new ObjectOutputStream(socket.getOutputStream());
             var ois = new ObjectInputStream(socket.getInputStream())) {
            // 睡眠是为了保证消息的有序发送，如果不睡眠由于数据传输需要时间会导致接收时是乱序的
            // 睡眠50ms就不乱了
            Thread.sleep(50);
            // 表明不停止
            oos.writeObject("ok");
            // 表明自己是一个Publisher
            oos.writeBoolean(true);
            oos.writeObject(msg);
            oos.flush();
            // 获取反馈，目的是如果消息队列已满，则在此处阻塞，直到可以发送新消息为止
            ois.readBoolean();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
