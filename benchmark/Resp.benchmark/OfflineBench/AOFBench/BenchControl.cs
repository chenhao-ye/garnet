// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Resp.benchmark
{
    /// <summary>
    /// Replica side of the bench control channel: a plain TCP line protocol that paces a remote
    /// Client-role process. The replica sends one ASCII line per event -- "BEGIN k" at measured
    /// pass start, "END k" at pass completion, "DONE" after the final pass -- and receives
    /// nothing: the client's TCP connect itself is its readiness signal. No parameter handshake;
    /// both processes are launched from the same experiment config, which check.py keeps
    /// consistent.
    /// </summary>
    public sealed class BenchControlServer : IDisposable
    {
        readonly TcpListener listener;
        TcpClient conn;
        StreamWriter writer;

        public BenchControlServer(int port)
        {
            listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
        }

        /// <summary>
        /// Blocks until the Client-role process connects.
        /// </summary>
        public void AcceptClient()
        {
            conn = listener.AcceptTcpClient();
            conn.NoDelay = true;
            writer = new StreamWriter(conn.GetStream(), Encoding.ASCII) { AutoFlush = true };
        }

        public void Send(string line) => writer.WriteLine(line);

        public void Dispose()
        {
            writer?.Dispose();
            conn?.Dispose();
            listener.Stop();
        }
    }

    /// <summary>
    /// Client side of the bench control channel: connects to the Replica-role process (with
    /// retry while it starts up) and reads pacing lines.
    /// </summary>
    public sealed class BenchControlClient : IDisposable
    {
        readonly TcpClient conn;
        readonly StreamReader reader;

        BenchControlClient(TcpClient conn)
        {
            this.conn = conn;
            this.reader = new StreamReader(conn.GetStream(), Encoding.ASCII);
        }

        /// <summary>
        /// Connects to the replica's control listener, retrying until it is up or the timeout
        /// elapses.
        /// </summary>
        public static BenchControlClient Connect(string host, int port, int timeoutSeconds)
        {
            var deadline = Stopwatch.GetTimestamp() + timeoutSeconds * Stopwatch.Frequency;
            while (true)
            {
                try
                {
                    var conn = new TcpClient(host, port) { NoDelay = true };
                    return new BenchControlClient(conn);
                }
                catch (SocketException)
                {
                    if (Stopwatch.GetTimestamp() > deadline)
                        throw new Exception($"BenchControlClient: replica control port {host}:{port} not reachable after {timeoutSeconds}s");
                    Thread.Sleep(250);
                }
            }
        }

        /// <summary>
        /// Blocks for the next control line; null when the replica closed the channel.
        /// </summary>
        public string ReadLine() => reader.ReadLine();

        public void Dispose()
        {
            reader.Dispose();
            conn.Dispose();
        }
    }
}