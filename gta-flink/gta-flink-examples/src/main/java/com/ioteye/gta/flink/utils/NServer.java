package com.ioteye.gta.flink.utils;

public class NServer {
    public static void main(String[] args) throws Exception {
        CommonSocketServer socketServer = new CommonSocketServer(9999);
        socketServer.run();
    }
}
