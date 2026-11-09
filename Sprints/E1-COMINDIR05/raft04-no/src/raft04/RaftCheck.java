package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public final class RaftCheck {
    private RaftCheck() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: RaftCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int count = Integer.parseInt(args[0]);
        Path folder = Paths.get(args[1]);
        if (!Files.isDirectory(folder)) {
            throw new IllegalArgumentException("Pasta inexistente: " + folder);
        }
        List<Path> files = Files.list(folder).sorted(Comparator.comparing(Path::getFileName)).collect(Collectors.toList());
        StringBuilder report = new StringBuilder();
        report.append("RaftCheck\n");
        report.append("Particoes: ").append(count).append("\n");
        report.append("Ficheiros lidos: ").append(files.size()).append("\n");
        System.out.print(report);
    }
}
