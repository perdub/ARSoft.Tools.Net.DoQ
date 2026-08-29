using System;
using System.IO;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ARSoft.Tools.Net.Dns;

public class QuicServerTransport : IServerTransport
{
    private readonly IPEndPoint _endpoint;
    private readonly QuicServerConnectionOptions _options;
    private QuicListener? _listener;
    private readonly Channel<QuicServerConnection> _connectionQueue = Channel.CreateUnbounded<QuicServerConnection>();
    private CancellationTokenSource _cts = new();

    public ushort DefaultAllowedResponseSize => ushort.MaxValue;
    public bool SupportsMultipleResponses => true;
    public bool AllowTruncatedResponses => false;
    public TransportProtocol TransportProtocol => TransportProtocol.Quic;

    private int _timeoutMilliseconds;
    public QuicServerTransport(IPEndPoint endpoint, SslServerAuthenticationOptions sslOptions, int maxInboundBidirectionalStreams = 32768, int timeout = 5000)
    {
        _endpoint = endpoint;
        _timeoutMilliseconds = timeout;
        if (sslOptions.ApplicationProtocols == null || !sslOptions.ApplicationProtocols.Contains(new SslApplicationProtocol("doq")))
        {
            sslOptions.ApplicationProtocols = new List<SslApplicationProtocol> { new SslApplicationProtocol("doq") };
        }

        _options = new QuicServerConnectionOptions
        {
            DefaultStreamErrorCode = 0,
            DefaultCloseErrorCode = 0,
            MaxInboundBidirectionalStreams = maxInboundBidirectionalStreams, 
            MaxInboundUnidirectionalStreams = 0,
            ServerAuthenticationOptions = sslOptions
        };
    }

    public async void Bind()
    {
        _listener = await QuicListener.ListenAsync(new QuicListenerOptions
        {
            ListenEndPoint = _endpoint,
            ApplicationProtocols = _options.ServerAuthenticationOptions.ApplicationProtocols!,
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(_options)
        });

        _ = Task.Run(AcceptConnectionsLoop, _cts.Token);
    }

    private async Task AcceptConnectionsLoop()
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var connection = await _listener!.AcceptConnectionAsync(_cts.Token);
                _ = Task.Run(() => ProcessConnectionAsync(connection), _cts.Token);
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task ProcessConnectionAsync(QuicConnection connection)
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var stream = await connection.AcceptInboundStreamAsync(_cts.Token);

                _ = Task.Run(() => HandleStreamAsync(stream, connection), _cts.Token);
            }
        }
        catch
        {
            _ = connection.DisposeAsync().AsTask();
        }
    }

    private async Task HandleStreamAsync(QuicStream stream, QuicConnection connection)
    {
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            timeoutCts.CancelAfter(_timeoutMilliseconds);

            var lengthBuf = new byte[2];
            await ReadExactAsync(stream, lengthBuf, timeoutCts.Token);
            int length = (lengthBuf[0] << 8) | lengthBuf[1];

            var payloadBuf = new byte[length];
            await ReadExactAsync(stream, payloadBuf, timeoutCts.Token);

            var fullBuf = new byte[length + 2];
            lengthBuf.CopyTo(fullBuf, 0);
            payloadBuf.CopyTo(fullBuf, 2);

            var rawPackage = new DnsReceivedRawPackage(fullBuf, (IPEndPoint)connection.RemoteEndPoint, (IPEndPoint)connection.LocalEndPoint);
            
            var serverCon = new QuicServerConnection(this, stream, rawPackage);
            await _connectionQueue.Writer.WriteAsync(serverCon, _cts.Token);
        }
        catch
        {
            _ = stream.DisposeAsync().AsTask();
        }
    }

    private static async Task ReadExactAsync(Stream stream, byte[] buffer, CancellationToken token)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(totalRead), token);
            if (read == 0) throw new EndOfStreamException();
            totalRead += read;
        }
    }

    public void Close()
    {
        _cts.Cancel();
        if (_listener != null) _ = _listener.DisposeAsync().AsTask();
    }

    public async Task<IServerConnection?> AcceptConnectionAsync(CancellationToken token = default)
    {
        try
        {
            await _connectionQueue.Reader.WaitToReadAsync(token);
            return _connectionQueue.Reader.TryRead(out var connection) ? connection : null;
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    public void Dispose() => Close();

    private class QuicServerConnection : IServerConnection
    {
        private readonly QuicStream _stream;
        private readonly DnsReceivedRawPackage _query;

        public IServerTransport Transport { get; }
        public IPEndPoint RemoteEndPoint => _query.RemoteEndpoint;
        public IPEndPoint LocalEndPoint => _query.LocalEndpoint;
        public bool CanRead { get; private set; } = true;

        public QuicServerConnection(IServerTransport transport, QuicStream stream, DnsReceivedRawPackage query)
        {
            Transport = transport;
            _stream = stream;
            _query = query;
        }

        public Task<bool> InitializeAsync(CancellationToken token = default) => Task.FromResult(true);

        public Task<DnsReceivedRawPackage?> ReceiveAsync(CancellationToken token = default)
        {
            if (CanRead)
            {
                CanRead = false;
                return Task.FromResult<DnsReceivedRawPackage?>(_query);
            }
            return Task.FromResult<DnsReceivedRawPackage?>(null);
        }

        public async Task<bool> SendAsync(DnsRawPackage package, CancellationToken token = default)
        {
            try
            {
                await _stream.WriteAsync(package.ToArraySegment(true).ToArray(), token);
                _stream.CompleteWrites();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void Dispose()
        {
            _ = _stream.DisposeAsync().AsTask();
        }
    }
}