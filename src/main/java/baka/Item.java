/*
 * Copyright (C) 2018 HYisen <alexhyisen@gmail.com>
 */

package baka;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Item {
    LocalDateTime time;
    String key;
    String url;

    public Item(LocalDateTime time, String key, String url) {
        this.time = time;
        this.key = key;
        this.url = url;
    }

    public Item(String line) {
        String[] words = line.split("\t");
        this.time = LocalDateTime.parse(words[0], DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        this.key = words[2];
        this.url = words[5];
    }

    @Override
    public String toString() {
        return time.format(DateTimeFormatter.ofPattern("ddHHmmss")) + "\t" +
                key + "\t" + url;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public String getKey() {
        return key;
    }

    public String getUrl() {
        return url;
    }
}
