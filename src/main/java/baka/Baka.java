package baka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Baka {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("/","home", "alex", "code", "00", "data");

        long count = Files
                .lines(path)
                .map(Item::new)
                .map(Item::getKey)
                .filter("人体艺术"::equals)
                .count();
        System.out.println(count);
    }
}
