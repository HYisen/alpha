import java.io.IOException;
import java.net.URISyntaxException;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {
        Analyse.go("keyword", v -> v.split("\t")[2], 30, false);
        Analyse.go("website", v -> v.split("\t")[5].split("/")[2], 10, true);
        Analyse.go("domain", v -> v.split("\t")[5].split("/")[2].split("\\.")[1], 10, true);
    }
}
