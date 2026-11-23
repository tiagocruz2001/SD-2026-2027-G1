package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LeaderBot extends ListenerAdapter {

    private static final String CANAL_ARQUIVO = "arquivo";
    private static final String PAPEL_ADMIN = "admin-arquivo";
    private static final Set<String> CMDS_ADMIN =
            Set.of("config", "replica-token", "replica-remover", "rajada");
    private final Path logPath = Path.of("registos.log");
    private final Path eventosPath = Path.of("eventos-lider.txt");
    private final Path masterKeyPath = Path.of("master.key");
    private final Path seladosPath = Path.of("selados");
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();
    private byte[] masterKey;
    private long ultimoLsn;
    private Chunk chunkAberto;

    public LeaderBot() throws IOException {
        masterKey = carregarOuCriarMasterKey();
        Files.createDirectories(seladosPath);
        ultimoLsn = lerUltimoLsn();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> scheduler.shutdownNow()));
    }

    public static void main(String[] args) throws Exception {
        String token = System.getenv("DISCORD_TOKEN");
        String guildId = System.getenv("GUILD_ID");
        if (token == null || token.isBlank() || guildId == null || guildId.isBlank()) {
            System.err.println("Define as variáveis de ambiente DISCORD_TOKEN e GUILD_ID.");
            System.exit(1);
        }

        LeaderBot leader = new LeaderBot();
        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(leader)
                .build()
                .awaitReady();

        Guild guild = jda.getGuildById(guildId);
        if (guild == null) {
            System.err.println("Servidor não encontrado. O bot foi convidado? GUILD_ID correto?");
            System.exit(1);
        }

        guild.updateCommands().addCommands(
                Commands.slash("registar", "Regista um texto no arquivo")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),
                Commands.slash("estado", "Mostra o estado do arquivo"),
                Commands.slash("config", "Configuração do arquivo (admin)")
                        .addSubcommands(new SubcommandData("r", "Define o nº de réplicas")
                                .addOptions(new net.dv8tion.jda.api.interactions.commands.build.OptionData(
                                        OptionType.INTEGER, "n", "Número de réplicas", true)
                                        .setMinValue(1))),
                Commands.slash("recuperar", "Recupera registos num intervalo")
                        .addOption(OptionType.INTEGER, "de", "Primeiro registo", true)
                        .addOption(OptionType.INTEGER, "ate", "Último registo", true),
                Commands.slash("replica-token", "Gera token de réplica (admin)"),
                Commands.slash("replica-remover", "Remove uma réplica (admin)")
                        .addOption(OptionType.STRING, "id", "ID da réplica", true),
                Commands.slash("rajada", "Envia uma rajada de pedidos (admin)")
                        .addOption(OptionType.INTEGER, "n", "Número de pedidos", true)
                        .addOption(OptionType.INTEGER, "intervaloms", "Intervalo em ms", true)
        ).queue();

        System.out.println("LeaderBot online e comandos registados.");
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent e) {
        String cmd = e.getName();

        if (CMDS_ADMIN.contains(cmd) && !isAdmin(e.getMember())) {
            e.reply("`/" + cmd + "` é restrito a membros com o papel `" + PAPEL_ADMIN + "`.")
                    .setEphemeral(true).queue();
            return;
        }

        switch (cmd) {
            case "registar" -> {
                String texto = e.getOption("texto").getAsString();
                String autor = e.getUser().getName();
                long lsn = registar(autor, "comando", texto);
                e.reply("Registo atribuído: LSN " + lsn).queue();
            }

            case "estado" -> e.reply("`/estado` recebido — ainda não implementado").queue();

            case "config" -> {
                if (!"r".equals(e.getSubcommandName())) {
                    erro(e, "Subcomando inválido. Usa `/config r <n>`.");
                    return;
                }
                long n = e.getOption("n").getAsLong();
                if (n < 1) {
                    erro(e, "`n` tem de ser um inteiro ≥ 1.");
                    return;
                }
                e.reply("`/config r` recebido: n=" + n + " — ainda não implementado").queue();
            }

            case "recuperar" -> {
                long de = e.getOption("de").getAsLong();
                long ate = e.getOption("ate").getAsLong();
                if (de < 1 || ate < de) {
                    erro(e, "Intervalo inválido: `de` deve ser ≥ 1 e ≤ `ate` (recebi de=" + de + ", ate=" + ate + ").");
                    return;
                }
                e.reply("`/recuperar` recebido: de=" + de + ", ate=" + ate
                        + " — ainda não implementado").queue();
            }

            case "replica-token" ->
                e.reply("`/replica-token` recebido — ainda não implementado").queue();

            case "replica-remover" -> {
                String id = e.getOption("id").getAsString().trim();
                if (id.isEmpty()) {
                    erro(e, "O `id` não pode estar vazio.");
                    return;
                }
                e.reply("`/replica-remover` recebido: id=" + id + " — ainda não implementado").queue();
            }

            case "rajada" -> {
                long n = e.getOption("n").getAsLong();
                long intervalo = e.getOption("intervaloms").getAsLong();
                if (n < 1 || intervalo < 0) {
                    erro(e, "`n` deve ser ≥ 1 e `intervaloMs` ≥ 0.");
                    return;
                }
                long ultimo = 0;
                for (long i = 1; i <= n; i++) {
                    ultimo = registar("rajada", CANAL_ARQUIVO,
                            "mensagem sintetica " + i);
                    if (intervalo > 0 && i < n) {
                        try {
                            Thread.sleep(intervalo);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            erro(e, "Rajada interrompida.");
                            return;
                        }
                    }
                }
                e.reply("Rajada concluída: " + n + " registos, último LSN " + ultimo).queue();
            }

            default -> erro(e, "Comando desconhecido.");
        }
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent e) {
        if (e.getAuthor().isBot()) return;
        if (!e.isFromGuild()) return;
        if (!e.getChannel().getName().equals(CANAL_ARQUIVO)) return;

        String texto = e.getMessage().getContentRaw();
        long lsn = registar(e.getAuthor().getName(), CANAL_ARQUIVO, texto);
        e.getMessage().reply("Registado com LSN " + lsn).queue();
    }

    public synchronized long registar(String autor, String canal, String texto) {
        if (texto == null) {
            throw new IllegalArgumentException("Texto não pode ser nulo");
        }

        long lsn = ultimoLsn + 1;
        Instant instante = Instant.now();
        Registo registo = new Registo(lsn, instante, autor, canal, texto);

        String textoBase64 = Base64.getEncoder()
                .encodeToString(texto.getBytes(StandardCharsets.UTF_8));
        String linha = lsn + ";" + instante + ";" + autor + ";" + canal + ";" + textoBase64;
        appendDuravel(logPath, linha);

        ultimoLsn = lsn;
        appendDuravel(eventosPath, instante + ";REGISTO;" + lsn);

        if (chunkAberto == null) {
            chunkAberto = new Chunk();
        }
        Chunk atual = chunkAberto;
        atual.add(registo);

        if (atual.getSerializedSize() >= Wire.CHUNK_MAX_BYTES) {
            selarChunk(atual);
        } else {
            agendarSelagem(atual);
        }

        return lsn;
    }

    private synchronized void agendarSelagem(Chunk chunk) {
        if (chunk.getCriadoEmMs() == 0) {
            return;
        }
        if (!chunk.consumeTimerScheduled()) {
            return;
        }
        scheduler.schedule(() -> {
            synchronized (this) {
                if (chunkAberto != chunk) {
                    return;
                }
                long elapsed = System.currentTimeMillis() - chunk.getCriadoEmMs();
                if (elapsed >= Wire.SELAGEM_MS) {
                    selarChunk(chunk);
                }
            }
        }, Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
    }

    private synchronized void selarChunk(Chunk chunk) {
        if (chunkAberto != chunk || chunk.isEmpty()) {
            return;
        }

        List<Registo> registos = chunk.getRegistos();
        byte[] padded = chunk.serialize();
        Sealed sealed = ChunkCrypto.sealPaddedChunk(padded, masterKey);

        Path bin = seladosPath.resolve(sealed.getChunkId() + ".bin");
        Path hdr = seladosPath.resolve(sealed.getChunkId() + ".hdr");
        writeDuravel(bin, sealed.getCipherText());
        String header = sealed.getKeyVersion() + ";"
                + ChunkCrypto.encodeBase64(sealed.getSalt()) + ";"
                + ChunkCrypto.encodeBase64(sealed.getNonce());
        appendDuravel(hdr, header);

        appendDuravel(eventosPath,
                Instant.now() + ";SELADO;"
                        + sealed.getChunkId() + ";LSN "
                        + registos.get(0).getLsn() + "-" + registos.get(registos.size() - 1).getLsn()
                        + ";tamanho " + sealed.getCipherText().length);

        chunkAberto = null;
    }

    private byte[] carregarOuCriarMasterKey() throws IOException {
        if (Files.exists(masterKeyPath)) {
            byte[] key = Files.readAllBytes(masterKeyPath);
            if (key.length != 32) {
                throw new IllegalArgumentException("master.key existe mas não tem 32 bytes");
            }
            return key;
        }

        byte[] key = new byte[32];
        java.security.SecureRandom secureRandom = new java.security.SecureRandom();
        secureRandom.nextBytes(key);

        try (FileChannel channel = FileChannel.open(masterKeyPath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            channel.write(ByteBuffer.wrap(key));
            channel.force(true);
        }

        try {
            Set<PosixFilePermission> ownerOnly = PosixFilePermissions.fromString("rw-------");
            Files.setPosixFilePermissions(masterKeyPath, ownerOnly);
        } catch (UnsupportedOperationException | SecurityException ignored) {
            // Permissões POSIX não estão disponíveis neste sistema operativo.
        }
        return key;
    }

    private long lerUltimoLsn() throws IOException {
        if (!Files.exists(logPath)) {
            return 0;
        }
        long ultimo = 0;
        for (String linha : Files.readAllLines(logPath, StandardCharsets.UTF_8)) {
            if (linha.isBlank()) continue;
            String[] partes = linha.split(";", 5);
            if (partes.length > 0) {
                try {
                    ultimo = Long.parseLong(partes[0].trim());
                } catch (NumberFormatException ignored) {
                    // Linhas inválidas não alteram a recuperação do último LSN.
                }
            }
        }
        return ultimo;
    }

    private static void appendDuravel(Path path, String line) {
        byte[] bytes = (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("Falha a gravar " + path, e);
        }
    }

    private static void writeDuravel(Path path, byte[] data) {
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("Falha a gravar " + path, e);
        }
    }

    private static boolean isAdmin(Member m) {
        return m != null && m.getRoles().stream().anyMatch(r -> r.getName().equals(PAPEL_ADMIN));
    }

    private static void erro(SlashCommandInteractionEvent e, String msg) {
        e.reply(msg).setEphemeral(true).queue();
    }
}