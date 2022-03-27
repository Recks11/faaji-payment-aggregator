package dev.faaji.streams.util;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class RandomUtils {

    private static final Random random = new Random();

    public static Random getRandom() {
        return random;
    }

    public static String randomGender() {
        String[] options = new String[]{"male", "female", "non-binary"};

        return options[random.nextInt(options.length)];
    }

    public static String generateString(int length) {
        var bytes = new byte[length];
        getRandom().nextBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
