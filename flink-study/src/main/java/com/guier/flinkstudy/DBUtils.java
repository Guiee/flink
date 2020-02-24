package com.guier.flinkstudy;

import java.util.Random;

public class DBUtils {
    public static int getConnection() {
        int conn = new Random().nextInt(10);
        return conn;
    }

    public static void returnConnection(int conn) {
        System.out.println("connection is: " + conn);
    }
}
