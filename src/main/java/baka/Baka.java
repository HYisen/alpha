/*
 * Copyright (C) 2018 HYisen <alexhyisen@gmail.com>
 */

package baka;

import utility.Stopwatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Baka {
    public static void main(String[] args) throws IOException {
        Stopwatch stopwatch = new Stopwatch();
//        Path path = Paths.get("/", "home", "alex", "code", "00", "data");

//        long count = Files
//                .lines(path)
//                .map(Item::new)
//                .map(Item::getKey)
//                .filter("人体艺术"::equals)
//                .count();
//        System.out.println(count);

//        Map<String, Long> data = new ConcurrentHashMap<>();
//        Files.lines(Paths.get("C:\\sogou.full.utf8"))
//                .parallel()
//                .map(v -> v.split("\t")[2])
//                .forEach(v -> data.put(v, data.getOrDefault(v, 0L) + 1));
//        stopwatch.report("load");
//        List<String> lines = data.entrySet().stream()
//                .parallel()
//                .sorted(Comparator.comparing(Map.Entry::getValue))
//                .map(v -> v.getValue() + "\t" + v.getKey())
//                .collect(Collectors.toList());
//        stopwatch.report("sort");
//        Files.write(Paths.get("result"), lines);
//        stopwatch.report("save");

        //a much more elegance way to achieve the target.
//        ConcurrentMap<String, List<Item>> data = Files.lines(path)
//                .parallel()
//                .map(Item::new)
//                .collect(Collectors.groupingByConcurrent(Item::getKey));
//        stopwatch.report("load");
//        List<String> lines = data.entrySet().stream()
//                .map(v -> v.getKey() + "\t" + v.getValue().size()).collect(Collectors.toList());
//        Files.write(Paths.get("output", "result"), lines);
//        stopwatch.report("save");


        Files.write(Paths.get("output"),
                Files.lines(Paths.get("C:\\sogou.full.utf8"))
                        .parallel()
                        .map(v -> v.split("\t")[2])
                        .collect(Collectors.groupingByConcurrent(Function.identity(), Collectors.counting()))
                        .entrySet().stream()
                        .parallel()
                        .sorted(Comparator.comparingLong(Map.Entry::getValue))
                        .map(v -> v.getKey() + "\t" + v.getValue()).collect(Collectors.toList()));

        stopwatch.report("completed");
    }
}
