package com.ioteye.gta.flink.utils;

public class NClient {
    public static void main(String[] args) throws Exception {
        CommonSendClient client = new CommonSendClient(9999);
        client.run();
    }
}
