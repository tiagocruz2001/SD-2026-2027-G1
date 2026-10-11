import java.net.*;
import java.io.*;
import java.util.Scanner;

public class UDPClient {

    private static final int SERVER_PORT = 6789;
    private static final String EXIT_WORD = "sair";

    public static void main(String args[]) {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Modo: auto ou manual");
            String mode = scanner.nextLine().trim().toLowerCase();

            int nextSequence = 1;

            while (true) {
                System.out.print("Mensagem (ou 'sair' para terminar): ");
                String input = scanner.nextLine();

                if (input.equalsIgnoreCase(EXIT_WORD)) {
                    break;
                }

                int sequence;

                if (mode.equals("auto") || mode.equals("automatic")) {
                    sequence = nextSequence++;
                } else if (mode.equals("manual")) {
                    System.out.print("Numero de sequencia: ");
                    String seqText = scanner.nextLine();
                    try {
                        sequence = Integer.parseInt(seqText.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Numero invalido. Tenta outra vez.");
                        continue;
                    }
                } else {
                    System.out.println("Modo invalido. Usa 'auto' ou 'manual'.");
                    break;
                }

                String message = sequence + "," + input;
                sendAndReceive(message);
            }
        }
    }

    private static void sendAndReceive(String message) {
        DatagramSocket aSocket = null;

        try {
            aSocket = new DatagramSocket();
            InetAddress aHost = InetAddress.getByName("localhost");

            byte[] m = message.getBytes();
            DatagramPacket request = new DatagramPacket(m, m.length, aHost, SERVER_PORT);
            aSocket.send(request);

            byte[] buffer = new byte[1000];
            DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
            aSocket.receive(reply);

            String response = new String(reply.getData(), 0, reply.getLength());

            if (response.startsWith("waitingfor,")) {
                System.out.println("WAITING FOR " + response.substring("waitingfor,".length()));
            } else {
                System.out.println("ECHO: " + response);
            }

        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
        }
    }
}