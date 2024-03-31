import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class WebServer {

    public static void main(String[] args) {
        try {
            int port = 8080; // Port number for the server
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Server is running on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept(); // Wait for client connection
                handleClientRequest(clientSocket); // Handle client request in a separate thread
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleClientRequest(Socket clientSocket) {
        new Thread(() -> {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                OutputStream out = clientSocket.getOutputStream();

                String requestLine = in.readLine(); // Read the request line
                if (requestLine != null && requestLine.startsWith("GET")) {
                    String response = "HTTP/1.1 200 OK\r\n" +
                            "Content-Type: text/html\r\n" +
                            "\r\n" +
                            "<!DOCTYPE html>\r\n" +
                            "<html>\r\n" +
                            "<head>\r\n" +
                            "<title>Simple Web Page</title>\r\n" +
                            "</head>\r\n" +
                            "<body>\r\n" +
                            "<h1>Hello from Java Web Server!</h1>\r\n" +
                            "</body>\r\n" +
                            "</html>";

                    out.write(response.getBytes(StandardCharsets.UTF_8));
                }

                out.flush();
                out.close();
                in.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }
}

