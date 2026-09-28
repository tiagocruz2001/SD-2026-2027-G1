import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDPServer {

    private static int L = 0;
    private static final Map<Integer, String> temporaryMessages = new HashMap<>();
    private static final List<String> deliveredMessages = new ArrayList<>();

    public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
        if (nCurrentMessage <= nLastMessageInOrder || temporaryMessages.containsKey(nCurrentMessage)) {
            return nLastMessageInOrder;
        }

        if (nCurrentMessage == nLastMessageInOrder + 1) {
            deliveredMessages.add(currentMessage);
            int nextDelivered = nCurrentMessage;

            while (temporaryMessages.containsKey(nextDelivered + 1)) {
                nextDelivered++;
                deliveredMessages.add(temporaryMessages.remove(nextDelivered));
            }

            return nextDelivered;
        }

        temporaryMessages.put(nCurrentMessage, currentMessage);
        return nLastMessageInOrder;
    }

    public static void main(String args[]) {
        DatagramSocket aSocket = null;

        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);

                String received = new String(request.getData(), 0, request.getLength());
                String[] parts = received.split(",", 2);

                if (parts.length != 2) {
                    continue;
                }

                try {
                    int sequence = Integer.parseInt(parts[0].trim());
                    String message = parts[1];

                    int previousL = L;
                    int newL = processDeliveredMessages(L, sequence, message);
                    String response;

                    if (newL == previousL) {
                        response = "waitingfor," + (previousL + 1);
                    } else {
                        response = received;
                    }

                    L = newL;
                    System.out.println("L=" + L + " | temporaria=" + temporaryMessages + " | entregues=" + deliveredMessages);

                    byte[] responseBytes = response.getBytes();
                    DatagramPacket reply = new DatagramPacket(
                            responseBytes,
                            responseBytes.length,
                            request.getAddress(),
                            request.getPort()
                    );

                    aSocket.send(reply);

                } catch (NumberFormatException e) {
                    continue;
                }
            }
        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); }
    }
}