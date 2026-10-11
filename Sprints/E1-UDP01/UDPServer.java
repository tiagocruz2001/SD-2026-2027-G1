import java.net.*;
import java.io.*;

public class UDPServer {

    private static int L = 0;

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

                    String response;

                    if (sequence == L + 1) {
                        L = sequence;
                        response = received;
                    } else {
                        response = "waitingfor," + (L + 1);
                    }

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