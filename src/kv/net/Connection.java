package kv.net;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/** A blocking request/response connection to one node. Not thread safe. */
public final class Connection implements AutoCloseable {

    private final Socket socket;
    private final BufferedReader in;
    private final BufferedWriter out;

    public Connection(String host, int port, int timeoutMillis) throws IOException {
        socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            socket.setTcpNoDelay(true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    public String request(String line) throws IOException {
        out.write(line);
        out.write('\n');
        out.flush();
        String response = in.readLine();
        if (response == null) {
            throw new IOException("Connection closed by peer");
        }
        return response;
    }

    /** Opens a connection, sends one request and closes it. */
    public static String send(String host, int port, int timeoutMillis, String line) throws IOException {
        try (Connection c = new Connection(host, port, timeoutMillis)) {
            return c.request(line);
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
