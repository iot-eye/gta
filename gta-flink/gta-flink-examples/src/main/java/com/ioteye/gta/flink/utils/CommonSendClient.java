package com.ioteye.gta.flink.utils;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.util.Scanner;

public class CommonSendClient {
    protected int port = 9999;

    public CommonSendClient(int port) {
        this.port = port;
    }

    public void run() throws Exception {
        Socket socket = new Socket("localhost", this.port);

        OutputStream outputStream = socket.getOutputStream();

        Scanner scanner = new Scanner(System.in);
        PrintStream printStream = new PrintStream(outputStream);

        boolean status = true;

        while (status) {
            System.out.print("input> ");
            String next = scanner.nextLine();
            printStream.println(next);
        }
    }
}


