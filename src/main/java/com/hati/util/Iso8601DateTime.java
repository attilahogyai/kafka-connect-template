package com.hati.util;


import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public final class Iso8601DateTime {

    public static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(UTC);

    public static ZonedDateTime parse(String timestamp) {

        ZonedDateTime zdt = ZonedDateTime.parse(timestamp, FORMATTER);
        zdt.truncatedTo(ChronoUnit.DAYS);
        return ZonedDateTime.ofInstant(zdt.toInstant(), UTC);
    }

    public static long parseToEpochMilli(String timestamp) {
        return parse(timestamp).toInstant().toEpochMilli();
    }

    public static String format(ZonedDateTime timestamp) {
        return FORMATTER.format(timestamp);
    }
    public static String formatToHour(ZonedDateTime timestamp) {
        ZonedDateTime truncated = timestamp.truncatedTo(ChronoUnit.HOURS);
        return HOUR_FORMATTER.format(truncated);
    }

    public static String formatEpochMilliToHour(long epochMilli) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), UTC);
        return formatToHour(zonedDateTime);
    }


}
