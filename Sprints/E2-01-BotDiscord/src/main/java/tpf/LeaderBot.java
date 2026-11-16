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

import java.util.Set;

public class LeaderBot extends ListenerAdapter {

    private static final String CANAL_ARQUIVO = "arquivo";
    private static final String PAPEL_ADMIN = "admin-arquivo";
    private static final Set<String> CMDS_ADMIN =
            Set.of("config", "replica-token", "replica-remover", "rajada");

    public static void main(String[] args) throws Exception {
        String token = System.getenv("DISCORD_TOKEN");
        String guildId = System.getenv("GUILD_ID");
        if (token == null || token.isBlank() || guildId == null || guildId.isBlank()) {
            System.err.println("Define as variáveis de ambiente DISCORD_TOKEN e GUILD_ID.");
            System.exit(1);
        }

        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new LeaderBot())
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
            case "registar" -> e.reply("`/registar` recebido: texto=\""
                    + e.getOption("texto").getAsString() + "\" — ainda não implementado").queue();

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
                e.reply("`/rajada` recebido: n=" + n + ", intervaloMs=" + intervalo
                        + " — ainda não implementado").queue();
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
        System.out.println("[#arquivo] " + e.getAuthor().getName() + ": " + texto);
        e.getMessage().reply("Mensagem detetada em #arquivo (\"" + texto
                + "\") — registo ainda não implementado").queue();
    }

    private static boolean isAdmin(Member m) {
        return m != null && m.getRoles().stream().anyMatch(r -> r.getName().equals(PAPEL_ADMIN));
    }

    private static void erro(SlashCommandInteractionEvent e, String msg) {
        e.reply(msg).setEphemeral(true).queue();
    }
}