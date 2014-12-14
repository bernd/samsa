package com.github.bernd.samsa.utils;

public class Os {
    public static String name() {
        return System.getProperty("os.name").toLowerCase();
    }

    public static boolean isWindows() {
        return name().startsWith("windows");
    }
}
