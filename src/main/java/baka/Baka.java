package baka;

import utility.Stopwatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Baka {
    public static void main(String[] args) throws IOException {
        Stopwatch stopwatch = new Stopwatch();
        Path path = Paths.get("/", "home", "alex", "code", "00", "data");

//        long count = Files
//                .lines(path)
//                .map(Item::new)
//                .map(Item::getKey)
//                .filter("人体艺术"::equals)
//                .count();
//        System.out.println(count);

        Map<String, Long> data = new ConcurrentHashMap<>();
        Files.lines(path)
                .parallel()
                .map(Item::new)
                .map(Item::getKey)
                .forEach(v -> data.put(v, data.getOrDefault(v, 0L) + 1));
        stopwatch.report("load");
        List<String> lines = data.entrySet().stream()
                .parallel()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .map(v -> v.getValue() + "\t" + v.getKey())
                .collect(Collectors.toList());
        stopwatch.report("sort");
        Files.write(Paths.get("output","result"), lines);
        stopwatch.report("save");
    }
}
