using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Mach.Utility;
using Mach.Data.MachClient;
using System.Threading;
using System.Net.Sockets;

namespace Mach.Core
{
    public sealed class SocketHandler
    {
        public SocketHandler(Socket socket)
        {
            m_socket = socket;
            RemainingTimeout = Constants.InfiniteTimeout; // msec
        }

        public void Dispose() => m_socket.Dispose();

        public int RemainingTimeout
        {
            get { return m_remainingTimeout; }
            set {
                m_remainingTimeout = value;
                // send/receiveTimeout is milisecond unit
                m_socket.SendTimeout = m_remainingTimeout;
                m_socket.ReceiveTimeout = m_remainingTimeout;
            }
        }

        public bool IsSuccess
        {
            get
            {
                if (Constants.NetworkSuccessRatio >= 100) { 
                    return true;
                } else { 
                    Random r = new Random();
                    int generate = r.Next(0, 10000) / 100;
                    return (generate <= Constants.NetworkSuccessRatio);
                }
            }
        }

        /* unused
        public int ReadBytes(byte[] aBuffer)
        {
            int timeout = Math.Min(int.MaxValue / 1000, RemainingTimeout);
            int remaining = RemainingTimeout;

            if (remaining == Constants.InfiniteTimeout)
                return m_socket.Receive(aBuffer, 0, aBuffer.Length, SocketFlags.None);

            while (remaining > 0)
            {
                var startTime = Environment.TickCount;
                if (m_socket.Poll(Math.Min(int.MaxValue / 1000, remaining) * 1000, SelectMode.SelectRead))
                {
                    return m_socket.Receive(aBuffer, 0, aBuffer.Length, SocketFlags.None);
                }
                remaining -= unchecked(Environment.TickCount - startTime);
            }
            return 0; // TODO debug
            // TIMEOUT_DURING_RECEIVE
            // throw new MachException(String.Format("Timeout is occurred during reading socket ({0} second(s) elapsed)", (float)(timeout / 1000)));
        }
        */

        public int ReadBytes(ArraySegment<byte> aBuffer)
        {
            int timeout = Math.Min(int.MaxValue / 1000, RemainingTimeout);
            int remaining = RemainingTimeout;

            if (remaining == Constants.InfiniteTimeout)
            {
                if (IsSuccess)
                    return m_socket.Receive(aBuffer.Array, aBuffer.Offset, aBuffer.Count, SocketFlags.None);
                else
                    throw new SocketException(10045);
            }

            while (remaining > 0)
            {
                var startTime = Environment.TickCount;
                if (m_socket.Poll(Math.Min(int.MaxValue / 1000, remaining) * 1000, SelectMode.SelectRead))
                {
                    if (IsSuccess)
                        return m_socket.Receive(aBuffer.Array, aBuffer.Offset, aBuffer.Count, SocketFlags.None);
                    else
                        throw new SocketException(10045);
                }
                remaining -= unchecked(Environment.TickCount - startTime);
            }

            throw new SocketException(10060); // ConnectionTimedOut
        }

        public int WriteBytes(ArraySegment<byte> aBuffer)
        {
            if (IsSuccess)
                return m_socket.Send(aBuffer.Array, aBuffer.Offset, aBuffer.Count, SocketFlags.None);
            else
                throw new SocketException(10045);
        }

        public int WriteBytes(byte[] aBytes)
        {
            if (IsSuccess)
                return m_socket.Send(aBytes, 0, aBytes.Length, SocketFlags.None);
            else
                throw new SocketException(10045);
        }

        readonly Socket m_socket;
        private int m_remainingTimeout;
    }
}
