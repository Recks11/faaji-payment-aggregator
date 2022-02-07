package dev.faaji.streams.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class TestDecimalUtils {

    public static BigDecimal decimalValue(double num) {
        return decimalValue(String.valueOf(num));
    }

    public static BigDecimal decimalValue(String num) {
        return new BigDecimal(num).setScale(2, RoundingMode.HALF_UP);
    }
}
