package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.IOUtils;
import redis.clients.jedis.util.RedisInputStream;
import redis.clients.jedis.util.RedisOutputStream;
import redis.clients.jedis.util.SafeEncoder;

// Distributed tracing and monitoring
import redis.clients.jedis.Observability;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;

public class Connection implements Closeable {

  private static final byte[][] EMPTY_ARGS = new byte[0][];

  private String host = Protocol.DEFAULT_HOST;
  private int port = Protocol.DEFAULT_PORT;
  private Socket socket;
  private RedisOutputStream outputStream;
  private RedisInputStream inputStream;
  private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
  private int soTimeout = Protocol.DEFAULT_TIMEOUT;
  private boolean broken = false;
  private boolean ssl;
  private SSLSocketFactory sslSocketFactory;
  private SSLParameters sslParameters;
  private HostnameVerifier hostnameVerifier;

  public Connection() {
  }

  public Connection(final String host) {
    this.host = host;
  }

  public Connection(final String host, final int port) {
    this.host = host;
    this.port = port;
  }

  public Connection(final String host, final int port, final boolean ssl) {
    this.host = host;
    this.port = port;
    this.ssl = ssl;
  }

  public Connection(final String host, final int port, final boolean ssl,
      SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier) {
    this.host = host;
    this.port = port;
    this.ssl = ssl;
    this.sslSocketFactory = sslSocketFactory;
    this.sslParameters = sslParameters;
    this.hostnameVerifier = hostnameVerifier;
  }

  public Socket getSocket() {
    return socket;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public int getSoTimeout() {
    return soTimeout;
  }

  public void setConnectionTimeout(int connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  public void setSoTimeout(int soTimeout) {
    this.soTimeout = soTimeout;
  }

  public void setTimeoutInfinite() {
    try {
      if (!isConnected()) {
        connect();
      }
      socket.setSoTimeout(0);
    } catch (SocketException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  public void rollbackTimeout() {
    try {
      socket.setSoTimeout(soTimeout);
    } catch (SocketException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  public void sendCommand(final ProtocolCommand cmd, final String... args) {
    final byte[][] bargs = new byte[args.length][];
    for (int i = 0; i < args.length; i++) {
      bargs[i] = SafeEncoder.encode(args[i]);
    }
    sendCommand(cmd, bargs);
  }

  public void sendCommand(final ProtocolCommand cmd) {
    sendCommand(cmd, EMPTY_ARGS);
  }

  public void sendCommand(final ProtocolCommand cmd, final byte[]... args) {
    Span span = Observability.startSpan("redis.Connection.sendCommand");

    try {
      connect();
      Protocol.sendCommand(outputStream, cmd, args);
    } catch (JedisConnectionException ex) {
      /*
       * When client send request which formed by invalid protocol, Redis send back error message
       * before close connection. We try to read it to provide reason of failure.
       */
      try {
        String errorMessage = Protocol.readErrorLineIfPossible(inputStream);
        if (errorMessage != null && errorMessage.length() > 0) {
          ex = new JedisConnectionException(errorMessage, ex.getCause());
        }
      } catch (Exception e) {
        /*
         * Catch any IOException or JedisConnectionException occurred from InputStream#read and just
         * ignore. This approach is safe because reading error message is optional and connection
         * will eventually be closed.
         */
      }
      // Any other exceptions related to connection?
      broken = true;
      span.setStatus(Status.INTERNAL.withDescription(ex.toString()));
      throw ex;
    } finally {
      span.end();
    }
  }

  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(final int port) {
    this.port = port;
  }

  public void connect() {
    if (isConnected()) {
      return;
    }

    Span span = Observability.startSpan("redis.Connection.connect");
    long startDialTimeNs = System.nanoTime();

    try {
        socket = new Socket();
        // ->@wjw_add
        socket.setReuseAddress(true);
        socket.setKeepAlive(true); // Will monitor the TCP connection is
        // valid
        socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to
        // ensure timely delivery of data
        socket.setSoLinger(true, 0); // Control calls close () method,
        // the underlying socket is closed
        // immediately
        // <-@wjw_add

        Observability.annotateSpan(span, "Connecting", 
                Observability.createAttribute("connection_timeout", connectionTimeout),
                Observability.createAttribute("keep_alive", true),
                Observability.createAttribute("no_tcp_delay", true),
                Observability.createAttribute("reuse_address", true),
                Observability.createAttribute("so_timeout", soTimeout),
                Observability.createAttribute("ssl", ssl));

        span.addAnnotation("Connecting");
        socket.connect(new InetSocketAddress(host, port), connectionTimeout);
        socket.setSoTimeout(soTimeout);
        span.addAnnotation("Connected");

        if (ssl) {
          if (null == sslSocketFactory) {
            sslSocketFactory = (SSLSocketFactory)SSLSocketFactory.getDefault();
          }
          socket = sslSocketFactory.createSocket(socket, host, port, true);
          if (null != sslParameters) {
            ((SSLSocket) socket).setSSLParameters(sslParameters);
          }
          if ((null != hostnameVerifier) &&
              (!hostnameVerifier.verify(host, ((SSLSocket) socket).getSession()))) {
            String message = String.format(
                "The connection to '%s' failed ssl/tls hostname verification.", host);
            span.setStatus(Status.INTERNAL.withDescription(message));
            throw new JedisConnectionException(message);
          }
        }

        outputStream = new RedisOutputStream(socket.getOutputStream());
        inputStream = new RedisInputStream(socket.getInputStream());
        Observability.recordStat(Observability.MConnectionsOpened, 1);
        span.addAnnotation("Created input and output streams");
    } catch (IOException ex) {
        broken = true;
        String message = String.format("Failed connecting to host " 
            + host + ":" + port, ex);
        span.setStatus(Status.INTERNAL.withDescription(message));
        throw new JedisConnectionException(message);
    } finally {
        long totalDialTimeNs = System.nanoTime() - startDialTimeNs;
        double timeSpentMilliseconds = (new Double(totalDialTimeNs))/1e6;
        Observability.recordStat(Observability.MDialLatencyMilliseconds, timeSpentMilliseconds);
        span.end();
    }
  }

  @Override
  public void close() {
    disconnect();
  }

  public void disconnect() {
    if (!isConnected())
        return;

    Span span = Observability.startSpan("redis.Connection.disconnect");

    try {
      outputStream.flush();
      socket.close();
      Observability.recordStat(Observability.MConnectionsClosed, 1);
    } catch (IOException ex) {
      broken = true;
      Observability.recordStat(Observability.MConnectionsClosedErrors, new Long(1));
      throw new JedisConnectionException(ex);
    } finally {
      IOUtils.closeQuietly(socket);
      span.end();
    }
  }

  public boolean isConnected() {
    return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
        && !socket.isInputShutdown() && !socket.isOutputShutdown();
  }

  public String getStatusCodeReply() {
    flush();
    final byte[] resp = (byte[]) readProtocolWithCheckingBroken();
    if (null == resp) {
      return null;
    } else {
      return SafeEncoder.encode(resp);
    }
  }

  public String getBulkReply() {
    final byte[] result = getBinaryBulkReply();
    if (null != result) {
      return SafeEncoder.encode(result);
    } else {
      return null;
    }
  }

  public byte[] getBinaryBulkReply() {
    flush();
    return (byte[]) readProtocolWithCheckingBroken();
  }

  public Long getIntegerReply() {
    flush();
    return (Long) readProtocolWithCheckingBroken();
  }

  public List<String> getMultiBulkReply() {
    return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
  }

  @SuppressWarnings("unchecked")
  public List<byte[]> getBinaryMultiBulkReply() {
    flush();
    return (List<byte[]>) readProtocolWithCheckingBroken();
  }

  @SuppressWarnings("unchecked")
  public List<Object> getRawObjectMultiBulkReply() {
    flush();
    return (List<Object>) readProtocolWithCheckingBroken();
  }

  public List<Object> getObjectMultiBulkReply() {
    return getRawObjectMultiBulkReply();
  }

  @SuppressWarnings("unchecked")
  public List<Long> getIntegerMultiBulkReply() {
    flush();
    return (List<Long>) readProtocolWithCheckingBroken();
  }

  public Object getOne() {
    flush();
    return readProtocolWithCheckingBroken();
  }

  public boolean isBroken() {
    return broken;
  }

  protected void flush() {
    Span span = Observability.startSpan("redis.Connection.flush");

    try {
      outputStream.flush();
    } catch (IOException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    } finally {
      span.end();
    }
  }

  protected Object readProtocolWithCheckingBroken() {
    Span span = Observability.startSpan("redis.Connection.readProtocolWithCheckingBroken");

    try {
      return Protocol.read(inputStream);
    } catch (JedisConnectionException exc) {
      span.setStatus(Status.INTERNAL.withDescription(exc.toString()));
      broken = true;
      throw exc;
    } finally {
      span.end();
    }
  }

  public List<Object> getMany(final int count) {
    Span span = Observability.startSpan("redis.Connection.getMany");

    try {
        span.addAnnotation("Invoking flush");
        flush();
        span.addAnnotation("Flushed");

        Observability.annotateSpan(span, "Getting responses back", new Observability.Attribute("count", count));
        final List<Object> responses = new ArrayList<Object>(count);
        for (int i = 0; i < count; i++) {
          try {
            responses.add(readProtocolWithCheckingBroken());
          } catch (JedisDataException e) {
            responses.add(e);
          }
        }
       return responses;
    } finally {
        span.end();
    }
  }
}
