package com.bigdata.sparkDstreming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class scoket {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(9999);
        Socket st = serverSocket.accept();

        BufferedReader br = new BufferedReader(new InputStreamReader(st.getInputStream()));

        PrintWriter writer = new PrintWriter(st.getOutputStream());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));

        while (true){
            String line = bufferedReader.readLine();
            //System.out.println("socket:read a line ->" + line);
            writer.println(line);
            writer.flush();
        }

    }
}
