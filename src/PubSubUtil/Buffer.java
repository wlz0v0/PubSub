package PubSubUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 基于阻塞队列{@link LinkedBlockingQueue}的消息队列，
 * 用于存放Publisher和Subscriber的消息，由{@link BufferManager}管理。
 * 所有消息都通过Socket在本地传输，端口号为10000~19999中的随机数。
 *
 * @param <E> 消息队列的消息类型，必须与对应的{@link Publisher}或{@link Subscriber}一致
 * @author 武连增
 * @since 11
 */
class Buffer<E> implements Runnable {
    /**
     * 存储消息的消息队列，采用阻塞式消息队列，使得当消息入队而队列已满时
     * 等待队列不为满再入队，当消息出队而队列为空时等待队列不为空再出列
     */
    private final LinkedBlockingQueue<E> msgQueue;

    /**
     * Socket服务器，接收Publisher和Subscriber的请求
     */
    private final ServerSocket ss;

    /**
     * Socket服务器的端口号
     */
    private final int PORT;

    private final String topic;

    /**
     * 执行与Publisher和Subscriber传输信息任务的线程池<br>
     * IO密集任务，线程数为2*CPU核心数
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors());

    /**
     * @param capacity 消息队列容量
     * @param port     Socket服务器端口号
     * @throws IOException 当遇到IO问题时
     */
    public Buffer(String topic, int capacity, int port) throws IOException {
        msgQueue = new LinkedBlockingQueue<>(capacity);
        this.topic = topic;
        this.PORT = port;
        ss = new ServerSocket(PORT);
    }

    /**
     * Buffer在后台运行方法，用于接收Publisher和Subscriber的连接请求及消息传输
     */
    @Override
    public void run() {
        while (!BufferManager.getInstance().stopFlag) {
            try {
                var socket = ss.accept();
                // 每一次publish或subscribe都会创建一个socket
                // 每个socket都创建一个线程会有很大的开销，于是选择使用线程池
                executor.execute(() -> {
                    try (var ois = new ObjectInputStream(socket.getInputStream());
                         var oos = new ObjectOutputStream(socket.getOutputStream())) {
                        // 是否是停止请求
                        String op = (String) ois.readObject();
                        if (op.equals("stop")) {
                            return;
                        }
                        // 读取连接的是否是Publisher
                        boolean isPub = ois.readBoolean();
                        if (isPub) {
                            // System.out.println("pub");
                            @SuppressWarnings("unchecked")
                            E msg = (E) ois.readObject();
                            publish(msg);
                            // 反馈接收成功，完成publish任务
                            oos.writeBoolean(true);
                            oos.flush();
                            // System.out.println(msg);
                        } else {
                            // System.out.println("sub");
                            // 等待Subscriber的读取请求
                            ois.readBoolean();
                            E msg = subscribe();
                            // System.out.println(msg);
                            oos.writeObject(msg);
                            oos.flush();
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            // 关闭服务端
            ss.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 关闭线程池
        executor.shutdownNow();
        System.out.println(topic + "话题已关闭");
    }

    /**
     * 获取端口号
     *
     * @return ServerSocket的端口号
     */
    public int getPort() {
        return PORT;
    }

    /**
     * 消息入队
     *
     * @param msg 入队的消息
     */
    void publish(E msg) {
        // 当等待时被打断就再put一次，如果是其他的异常则不put
        boolean flag;
        do {
            try {
                msgQueue.put(msg);
                flag = false;
            } catch (InterruptedException e) {
                flag = true;
                e.printStackTrace();
            }
        } while (flag);
    }

    /**
     * 消息出队
     *
     * @return 出队的消息
     */
    E subscribe() {
        E res = null;
        // 当等待时被打断就再take一次，直到成功为止
        do {
            try {
                res = msgQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (res == null);
        return res;
    }
}
