package dev.faaji.streams.util;

public class KeyUtils {


    public static String merge(String... strings) {
        if (strings.length == 0) return "";
        if (strings.length == 1) return strings[0];
        StringBuilder out = new StringBuilder(strings[0]);

        for (int i = 1; i < strings.length; i++) {
            out.append(":").append(strings[i]);
        }

        return out.toString();
    }
}
