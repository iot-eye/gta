package com.ioteye.gta.flink.utils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class CommonSocketServer {
    protected AtomicBoolean alreadyStatus = new AtomicBoolean(false);
    protected HashMap<Socket, PrintStream> socketOutPutMap = new HashMap<>();
    protected LinkedBlockingDeque<String> dataQueue = new LinkedBlockingDeque<>(100);
    protected int port = 9999;

    public CommonSocketServer(int port) {
        this.port = port;
    }

    public void run() throws Exception {
        System.out.println("localhost:port=" + port);
        ServerSocket serverSocket = new ServerSocket(this.port);
        Thread sendThread = new SendThread();
        sendThread.start();
        boolean status = true;
        while (status) {
            Socket socket = serverSocket.accept();
            new SocketThread(socket).start();
            System.out.println("client = " + socket.getRemoteSocketAddress() + " login");
        }
    }

    class SendThread extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    String msg = dataQueue.take();
                    Set<Map.Entry<Socket, PrintStream>> entrySet = socketOutPutMap.entrySet();
                    List<Socket> socketList = new ArrayList<>();
                    for (Map.Entry<Socket, PrintStream> entry : entrySet) {
                        try {
                            PrintStream printStream = entry.getValue();
                            printStream.println(msg);
                        } catch (Exception e) {
                            e.printStackTrace();
                            socketList.add(entry.getKey());
                        }
                    } for (Socket socket : socketList) {
                        socketOutPutMap.remove(socket);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class SocketThread extends Thread {
        Socket socket;

        SocketThread(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                OutputStream os = socket.getOutputStream();
                PrintStream ps = new PrintStream(os);
                if (alreadyStatus.compareAndSet(false, true)) {
                    InputStream is = socket.getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                    while (true) {
                        String msg = br.readLine();
                        if (msg.equalsIgnoreCase("bye")) {
                            ps.println("console> byebye");
                            socket.close();
                            socketOutPutMap.remove(socket);
                            break;
                        } else {
                            dataQueue.put(msg);
                        } System.out.println("console> " + msg);
                    }
                } socketOutPutMap.put(socket, ps);
            } catch (Exception e) {}
        }
    }
}

