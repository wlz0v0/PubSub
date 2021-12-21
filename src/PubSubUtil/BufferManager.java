package PubSubUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 消息队列管理器，用于向Publisher和Subscriber反馈
 * 消息队列服务器的端口号，以及管理{@link Buffer}。<br>
 * 在运行任何一个{@link Publisher}或{@link Subscriber}前必须先启动管理器{@link BufferManager#launch()}！
 * </p>
 *
 * @author 武连增
 * @since 11
 */
public class BufferManager {
    /**
     * 管理器端口号
     */
    static final int CORE_PORT = 9999;

    /**
     * 管理器单例，饿汉式
     */
    private final static BufferManager INSTANCE = new BufferManager();

    /**
     * 储存所有的Buffer，key为Buffer的话题名，value为Buffer实例
     */
    private final HashMap<String, Buffer<?>> bufferPool = new HashMap<>();

    /**
     * 用到的端口集合，端口从10000~19999随机生成
     */
    private final HashSet<Integer> ports = new HashSet<>();

    /**
     * 执行向Publisher和Subscriber发送端口号任务的线程池<br>
     * IO密集型任务，线程数为2*CPU核心数
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors());

    /**
     * 停止标志
     */
    volatile boolean stopFlag;

    /**
     * 管理器的ServerSocket，用于发送端口号信息
     */
    private ServerSocket ss = null;

    /**
     * 单例类，构造器私有
     */
    private BufferManager() {
        stopFlag = false;
        ports.add(CORE_PORT);
        try {
            ss = new ServerSocket(CORE_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取BufferManager的实例
     *
     * @return BufferManager的实例
     */
    public static BufferManager getInstance() {
        return INSTANCE;
    }

    /**
     * 启动管理器的方法，PubSub系统的核心
     */
    public void launch() {
        new Thread(() -> {
            // 当输入exit时结束核心程序
            System.out.println("请输入exit以停止程序运行");
            var scanner = new Scanner(System.in);
            String str;
            do {
                str = scanner.nextLine();
            } while (!str.equals("exit"));
            stopFlag = true;
            closeAll();
        }).start();

        while (!stopFlag) {
            try {
                var socket = ss.accept();
                executor.execute(() -> {
                    try (var ois = new ObjectInputStream(socket.getInputStream());
                         var oos = new ObjectOutputStream(socket.getOutputStream())) {
                        // 先接收话题名和容量
                        String topic = (String) ois.readObject();
                        if (topic.equals("stop")) {
                            return;
                        }
                        int capacity = ois.readInt();
                        var buffer = getOrCreateBuffer(topic, capacity);
                        // 再发送端口号
                        oos.writeInt(buffer.getPort());
                        oos.flush();
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
        System.out.println("核心已关闭");
    }

    /**
     * 获取对应话题名的Buffer，不存在则新建
     *
     * @param topic    话题名
     * @param capacity 消息队列容量
     * @param <E>      消息类型
     * @return 如果已存在话题名，则返回现有的Buffer；否则新建一个Buffer并返回
     */
    private <E> Buffer<E> getOrCreateBuffer(String topic, int capacity) {
        @SuppressWarnings("unchecked")
        var buffer = (Buffer<E>) bufferPool.get(topic);
        if (buffer == null) {
            try {
                buffer = new Buffer<>(topic, capacity, createPort());
                // 加入到map中以通过topic获取buffer
                bufferPool.put(topic, buffer);
                // 启动buffer，开始接收连接请求
                new Thread(buffer).start();
                System.out.println("启动话题: " + topic);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return buffer;
    }

    /**
     * 为Buffer创建不重复的端口号
     *
     * @return 端口号
     */
    private int createPort() {
        var random = new Random();
        int res;
        do {
            res = random.nextInt(10000) + 10000;
        } while (ports.contains(res));
        ports.add(res);
        return res;
    }

    /**
     * 关闭所有ServerSocket
     */
    private void closeAll() {
        ports.forEach(integer -> {
            try (var socket = new Socket("localhost", integer);
                 var oos = new ObjectOutputStream(socket.getOutputStream());
                 var ignored = new ObjectInputStream(socket.getInputStream())) {
                oos.writeObject("stop");
                oos.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
