package com.bigdata.sparkDstreming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class scoketclient {
    public static void main(String[] args) throws Exception {

        Socket st = new Socket("localhost", 9999);

        BufferedReader br = new BufferedReader(new InputStreamReader(st.getInputStream()));

        //PrintWriter writer = new PrintWriter(st.getOutputStream());

        while (true){
            System.out.println(br.readLine());
        }
    }
}
