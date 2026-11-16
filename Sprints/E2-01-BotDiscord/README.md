# 🤖 tpf-lider

> **LeaderBot** · Sistemas Distribuídos 2026-2027 · Grupo 1

Bot de Discord que funciona como líder do arquivo.

---

## 🔑 Configurar o token

O token do bot **nunca** vai para o repositório. Define-o como variável de ambiente, juntamente com o ID do servidor.

No **PowerShell/Terminal do VS Code**:

```powershell
setx DISCORD_TOKEN "o_token_do_bot"
setx GUILD_ID "id_do_servidor"
```

> 💡 Depois de correr os comandos, fecha e reabre o terminal (ou o VS Code) para as variáveis ficarem ativas.

---

## 🚀 Arrancar o líder

No **PowerShell/Terminal do VS Code**:

```powershell
mvn compile exec:java
```

Quando aparecer a mensagem abaixo, o bot está online:

```
LeaderBot online e comandos registados.
```

Para parar, usa `Ctrl+C`.