package baka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Baka {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("/","home", "alex", "code", "01", "data");

        Files
                .lines(path)
                .map(Item::new)
                .forEach(System.out::println);
    }
}
