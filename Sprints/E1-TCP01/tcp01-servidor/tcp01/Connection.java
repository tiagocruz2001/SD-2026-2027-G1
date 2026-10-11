package tcp01;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.io.ObjectInputStream;

public class Connection extends Thread {
    ObjectInputStream go;
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            go = new ObjectInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());
            this.start(); // executa run() numa thread separada
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            Person data = Person.class.cast(go.readObject()); // lê os dados do cliente
            out.writeUTF(data.getPlace().getLocality()); // envia a resposta ao cliente
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                /* falha ao fechar */
            }
        }
    }
}
