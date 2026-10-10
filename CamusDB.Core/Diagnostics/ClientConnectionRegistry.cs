/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// The client connections this node holds open now, read by <c>SHOW CONNECTIONS</c>.
///
/// <para><b>CamusDB has no session of its own, so a connection here is a transport
/// connection.</b> The host adds one entry when its web server accepts a TCP connection on a
/// client-facing listener and removes it when that connection closes. HTTP/2 carries many calls on
/// one connection, so a gRPC client with one channel is one row however many statements it runs at
/// once; the per-connection statement count comes from <see cref="QueryActivityRegistry"/>, which
/// links each statement to its connection.</para>
///
/// <para>The engine never adds entries itself. An engine with no host — the tests, the browser
/// playground — has an empty registry, and <c>SHOW CONNECTIONS</c> then returns no rows.</para>
/// </summary>
public sealed class ClientConnectionRegistry
{
    private readonly ConcurrentDictionary<string, ClientConnection> byTransportId = new(StringComparer.Ordinal);

    private readonly ActivityIdMinter ids;

    internal ClientConnectionRegistry(ActivityIdMinter ids)
    {
        this.ids = ids;
    }

    /// <summary>
    /// Adds a connection the host just accepted.
    /// </summary>
    /// <param name="transportConnectionId">
    /// The web server's own id for the connection. The host uses it to find this entry again from a
    /// request, because the request sees the same id.
    /// </param>
    /// <param name="clientAddress">The remote IP address as text, or null when the transport has none.</param>
    public ClientConnection Open(string transportConnectionId, string? clientAddress)
    {
        ClientConnection connection = new(this, ids.NextConnectionId(), transportConnectionId, clientAddress);
        byTransportId[transportConnectionId] = connection;
        return connection;
    }

    /// <summary>The open connection with the web server id <paramref name="transportConnectionId"/>, or null.</summary>
    public ClientConnection? Find(string transportConnectionId)
        => byTransportId.TryGetValue(transportConnectionId, out ClientConnection? connection) ? connection : null;

    /// <summary>The open connection with the display id <paramref name="connectionId"/>, or null.</summary>
    public ClientConnection? FindById(string connectionId)
    {
        foreach (KeyValuePair<string, ClientConnection> pair in byTransportId)
        {
            if (string.Equals(pair.Value.Id, connectionId, StringComparison.Ordinal))
                return pair.Value;
        }

        return null;
    }

    /// <summary>
    /// The number of connections open now. It takes every lock of the map, so it is for tests and
    /// diagnostics, never for the connection path.
    /// </summary>
    public int Count => byTransportId.Count;

    /// <summary>
    /// The connections open now, in no particular order. Enumerated without a lock, so a connection
    /// that opens or closes meanwhile may or may not appear; the web server is never stalled by a read.
    /// </summary>
    public IEnumerable<ClientConnection> List()
    {
        foreach (KeyValuePair<string, ClientConnection> pair in byTransportId)
            yield return pair.Value;
    }

    internal void Remove(ClientConnection connection)
        => byTransportId.TryRemove(new KeyValuePair<string, ClientConnection>(connection.TransportId, connection));
}

/// <summary>
/// One open client connection. Its counters are written by the request path and read by
/// <c>SHOW CONNECTIONS</c> on another thread, so every mutable field is interlocked or volatile.
///
/// <para><b>The user is one immutable pair, read once.</b> The user decides which viewers see the
/// row, so its name and id must never come from two different statements. One connection can carry
/// statements of several users, so <see cref="User"/> is replaced as a whole, and a reader takes
/// it once and uses that same value for the visibility check and for the row.</para>
/// </summary>
public sealed class ClientConnection
{
    private readonly ClientConnectionRegistry owner;

    private readonly long openedAtTicks;

    private long lastActivityTicks;

    private long requests;

    private int activeRequests;

    private int openStreams;

    private int peer;

    private int closed;

    private string? protocol;

    private ConnectionUser? user;

    private StatementOrigin? cachedOrigin;

    internal ClientConnection(ClientConnectionRegistry owner, string id, string transportId, string? clientAddress)
    {
        this.owner = owner;
        Id = id;
        TransportId = transportId;
        ClientAddress = clientAddress;
        OpenedAt = DateTime.UtcNow;
        openedAtTicks = Stopwatch.GetTimestamp();
        lastActivityTicks = openedAtTicks;
    }

    /// <summary>The id <c>SHOW CONNECTIONS</c> and <c>SHOW QUERIES</c> report. Unique across the cluster.</summary>
    public string Id { get; }

    /// <summary>The web server's own id for this connection.</summary>
    public string TransportId { get; }

    /// <summary>The remote IP address as text, or null.</summary>
    public string? ClientAddress { get; }

    /// <summary>When the connection was accepted, in UTC.</summary>
    public DateTime OpenedAt { get; }

    /// <summary>The HTTP protocol of the last request, for example <c>HTTP/1.1</c> or <c>HTTP/2</c>; null before the first request.</summary>
    public string? Protocol => Volatile.Read(ref protocol);

    /// <summary>
    /// True when a request on this connection went to an <c>/internal/</c> endpoint, which only a
    /// peer node calls. Peers share the client-facing HTTP listener, so the operator needs this to
    /// tell node-to-node traffic from client traffic.
    /// </summary>
    public bool IsPeer => Volatile.Read(ref peer) != 0;

    /// <summary>
    /// The user of the last authenticated statement on this connection, or null before the first
    /// one. Read it once per use: two reads can return two different users.
    /// </summary>
    public ConnectionUser? User => Volatile.Read(ref user);

    /// <summary>Requests started on this connection since it opened.</summary>
    public long Requests => Interlocked.Read(ref requests);

    /// <summary>Requests that run on this connection now.</summary>
    public int ActiveRequests => Volatile.Read(ref activeRequests);

    /// <summary><c>BatchExecute</c> streams open on this connection now.</summary>
    public int OpenStreams => Volatile.Read(ref openStreams);

    /// <summary>Milliseconds since the connection was accepted.</summary>
    public double AgeMs => Stopwatch.GetElapsedTime(openedAtTicks).TotalMilliseconds;

    /// <summary>
    /// Milliseconds since the last request or statement on this connection started, or since the
    /// last request ended, whichever is latest. The caller reports 0 instead while a statement runs.
    ///
    /// <para>It is measured from activity, not from open requests, because a gRPC
    /// <c>BatchExecute</c> stream is one request that stays open for the life of the client: a
    /// connection that holds one would otherwise never look idle, however long it sat with no
    /// statement. A large value on a connection that holds a transaction open is what an operator
    /// looks for.</para>
    /// </summary>
    public double IdleMs => Stopwatch.GetElapsedTime(Interlocked.Read(ref lastActivityTicks)).TotalMilliseconds;

    /// <summary>
    /// Marks the start of a request. The caller must call <see cref="RequestEnded"/> in a
    /// <c>finally</c>, or the connection reports a request that runs forever.
    /// </summary>
    public void RequestStarted(string? httpProtocol, bool internalPath)
    {
        Interlocked.Increment(ref requests);
        Interlocked.Increment(ref activeRequests);
        Interlocked.Exchange(ref lastActivityTicks, Stopwatch.GetTimestamp());

        if (httpProtocol is not null && !ReferenceEquals(Volatile.Read(ref protocol), httpProtocol))
            Volatile.Write(ref protocol, httpProtocol);

        if (internalPath)
            Volatile.Write(ref peer, 1);
    }

    /// <summary>Marks the end of a request started with <see cref="RequestStarted"/>.</summary>
    public void RequestEnded()
    {
        Interlocked.Decrement(ref activeRequests);
        Interlocked.Exchange(ref lastActivityTicks, Stopwatch.GetTimestamp());
    }

    /// <summary>
    /// Marks the start of a <c>BatchExecute</c> stream. The caller must call
    /// <see cref="StreamEnded"/> in a <c>finally</c>.
    /// </summary>
    public void StreamStarted() => Interlocked.Increment(ref openStreams);

    /// <summary>Marks the end of a stream started with <see cref="StreamStarted"/>.</summary>
    public void StreamEnded() => Interlocked.Decrement(ref openStreams);

    /// <summary>
    /// The statement origin for a request on this connection over <paramref name="transport"/>. The
    /// last one is cached, so a connection that keeps one transport — the usual case — builds it once
    /// rather than once per request.
    /// </summary>
    public StatementOrigin OriginFor(string transport)
    {
        StatementOrigin? origin = Volatile.Read(ref cachedOrigin);
        if (origin is not null && string.Equals(origin.Transport, transport, StringComparison.Ordinal))
            return origin;

        origin = new StatementOrigin(transport, this, ClientAddress);
        Volatile.Write(ref cachedOrigin, origin);
        return origin;
    }

    /// <summary>Records a statement that started on this connection, and its user.</summary>
    internal void NoteStatement(string? name, string? id)
    {
        Interlocked.Exchange(ref lastActivityTicks, Stopwatch.GetTimestamp());

        if (name is null)
            return;

        // The usual connection carries one user, so the pair is replaced only when it changes, and
        // the statement path allocates nothing.
        ConnectionUser? current = Volatile.Read(ref user);
        if (current is not null &&
            string.Equals(current.Name, name, StringComparison.Ordinal) &&
            string.Equals(current.Id, id, StringComparison.Ordinal))
            return;

        Volatile.Write(ref user, new ConnectionUser(name, id));
    }

    /// <summary>
    /// Removes the connection from the registry. Called by the host when the web server closes the
    /// connection. Safe to call more than once.
    /// </summary>
    public void Close()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0)
            return;

        owner.Remove(this);
    }
}

/// <summary>
/// The user of a connection: the name and the user id of one statement, published together so a
/// reader never mixes the name of one user with the id of another.
/// </summary>
public sealed record ConnectionUser(string Name, string? Id);
