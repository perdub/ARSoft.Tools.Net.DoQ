using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;

namespace ARSoft.Tools.Net.Dns;

public class QuicClientTransport : IClientTransport
{
    private readonly int _port;
    private readonly QuicClientConnectionOptions _options;
    private readonly ConcurrentDictionary<IPEndPoint, Lazy<Task<QuicConnection>>> _connectionPool = new();

    public ushort MaximumAllowedQuerySize => ushort.MaxValue;
    public bool SupportsReliableTransfer => true;
    public bool SupportsMulticastTransfer => false;
    public bool SupportsPooledConnections => true;

    public QuicClientTransport(SslClientAuthenticationOptions sslOptions, int port = 853)
    {
        _port = port;
        if (sslOptions.ApplicationProtocols == null || !sslOptions.ApplicationProtocols.Contains(new SslApplicationProtocol("doq")))
        {
            sslOptions.ApplicationProtocols = new List<SslApplicationProtocol> { new SslApplicationProtocol("doq") };
        }

        _options = new QuicClientConnectionOptions
        {
            DefaultStreamErrorCode = 0,
            DefaultCloseErrorCode = 0,
            MaxInboundBidirectionalStreams = 512,
            MaxInboundUnidirectionalStreams = 0,
            ClientAuthenticationOptions = sslOptions
        };
    }

    public async Task<IClientConnection?> ConnectAsync(DnsClientEndpointInfo endpointInfo, int queryTimeout, CancellationToken token = default)
    {
        var endpoint = new IPEndPoint(endpointInfo.DestinationAddress, _port);

        var lazyConn = _connectionPool.GetOrAdd(endpoint, ep => new Lazy<Task<QuicConnection>>(() =>
        {
            var connOptions = new QuicClientConnectionOptions
            {
                DefaultStreamErrorCode = _options.DefaultStreamErrorCode,
                DefaultCloseErrorCode = _options.DefaultCloseErrorCode,
                MaxInboundBidirectionalStreams = _options.MaxInboundBidirectionalStreams,
                MaxInboundUnidirectionalStreams = _options.MaxInboundUnidirectionalStreams,
                ClientAuthenticationOptions = _options.ClientAuthenticationOptions,
                RemoteEndPoint = ep // Устанавливаем целевой IP
            };
            return QuicConnection.ConnectAsync(connOptions, token).AsTask();
        }));

        try
        {
            var connection = await lazyConn.Value;
            return new QuicClientConnection(this, connection, queryTimeout, endpoint);
        }
        catch
        {
            _connectionPool.TryRemove(endpoint, out _);
            return null;
        }
    }

    public async Task<IClientConnection?> GetPooledConnectionAsync(DnsClientEndpointInfo endpointInfo, CancellationToken token = default)
    {
        var endpoint = new IPEndPoint(endpointInfo.DestinationAddress, _port);
        if (_connectionPool.TryGetValue(endpoint, out var lazyConn))
        {
            try
            {
                var connection = await lazyConn.Value;
                return new QuicClientConnection(this, connection, 5000, endpoint);
            }
            catch
            {
                _connectionPool.TryRemove(endpoint, out _);
            }
        }
        return null;
    }

    internal void RemoveFromPool(IPEndPoint endpoint)
    {
        _connectionPool.TryRemove(endpoint, out _);
    }

    public void Dispose()
    {
        foreach (var connLazy in _connectionPool.Values)
        {
            if (connLazy.IsValueCreated && connLazy.Value.IsCompletedSuccessfully)
            {
                _ = connLazy.Value.Result.DisposeAsync().AsTask();
            }
        }
        _connectionPool.Clear();
    }

    private class QuicClientConnection : IClientConnection
    {
        private readonly QuicClientTransport _transport;
        private readonly QuicConnection _connection;
        private readonly int _queryTimeout;
        private readonly IPEndPoint _endpoint;

        private QuicStream? _stream;
        private ushort _originalTxId;

        public IClientTransport Transport => _transport;
        public bool IsFaulty { get; private set; }

        public QuicClientConnection(QuicClientTransport transport, QuicConnection connection, int queryTimeout, IPEndPoint endpoint)
        {
            _transport = transport;
            _connection = connection;
            _queryTimeout = queryTimeout;
            _endpoint = endpoint;
        }

        public async Task<bool> SendAsync(DnsRawPackage package, CancellationToken token = default)
        {
            try
            {
                _stream = await _connection.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, token);
                var data = package.ToArraySegment(true).ToArray();

                _originalTxId = (ushort)((data[2] << 8) | data[3]);
                data[2] = 0; data[3] = 0;

                await _stream.WriteAsync(data, token);
                _stream.CompleteWrites();
                
                return true;
            }
            catch
            {
                MarkFaulty();
                return false;
            }
        }

        public async Task<DnsReceivedRawPackage?> ReceiveAsync(DnsMessageIdentification identification, CancellationToken token = default)
        {
            if (_stream == null) return null;

            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                timeoutCts.CancelAfter(_queryTimeout);

                var lengthBuf = new byte[2];
                await ReadExactAsync(_stream, lengthBuf, timeoutCts.Token);
                int length = (lengthBuf[0] << 8) | lengthBuf[1];

                var payloadBuf = new byte[length];
                await ReadExactAsync(_stream, payloadBuf, timeoutCts.Token);

                payloadBuf[0] = (byte)(_originalTxId >> 8);
                payloadBuf[1] = (byte)(_originalTxId & 0xFF);

                var fullBuf = new byte[length + 2];
                lengthBuf.CopyTo(fullBuf, 0);
                payloadBuf.CopyTo(fullBuf, 2);

                return new DnsReceivedRawPackage(fullBuf, _endpoint, (IPEndPoint)_connection.LocalEndPoint);
            }
            catch
            {
                MarkFaulty();
                return null;
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

        public void RestartIdleTimeout(TimeSpan? timeout) { }
        public void MarkFaulty() { IsFaulty = true; }

        public void Dispose()
        {
            if (_stream != null)
            {
                _ = _stream.DisposeAsync().AsTask(); 
            }
        }
    }
}