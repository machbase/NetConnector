using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using Mach.Data.MachClient;
using Mach.Core;
using System.IO;
using Mach.Comm;
using Mach.Utility;
using System.Threading;
using System.Threading.Tasks;

namespace Mach.Core
{
    public class Session
    {
        public enum State
        {
            // The session has been created; no connection has been made.
            Created,

            // Socket is established
            Inited,

            // Handshaking
            Connecting,

            // Active user is connected.
            Connected,

            // The session is closed.
            Closed,

            // An unexpected error occurred; the session is in an unusable state.
            Failed,
        }

        private State m_state;
        private TcpClient m_tcpClient;
        private Socket m_socket;

        public Session()
        {
            m_state = State.Created;
            // ConnectSocket("localhost");
        }

        public void Connect(ConnectionSettings aConnSetting)
        {
            foreach (var hostname in aConnSetting.HostNames)
            {
                IPAddress[] ipAddresses = Dns.GetHostAddresses(hostname);
                foreach (var ipAddress in ipAddresses)
                {
                    TcpClient tcpClient = null;
                    try
                    {
                        tcpClient = new TcpClient(ipAddress.AddressFamily);

                        // NOTE there was an issue on non-windows platforms.
                        // non-windows platforms block on synchronous connect, use send/receive timeouts: https://github.com/dotnet/corefx/issues/20954
                        tcpClient.Connect(ipAddress, aConnSetting.Port);

                        m_state = State.Inited;
                        m_tcpClient = tcpClient;
                        m_socket = m_tcpClient.Client;
                        m_payloadHandler = new ProtocolHandler(new SocketHandler(m_socket), new BufferedByteReader());
                        break;
                    }
                    catch (Exception ex)            
                    {
                        tcpClient?.Client?.Dispose();
                        SafeDispose(ref tcpClient);
                       
                        throw ex;
                    }
                }

                // TODO next hostname?
            }
        }

//#if !NET40
//        public void Connect(ConnectionSettings aConnSetting)
//        {
//            foreach (var hostname in aConnSetting.HostNames)
//            {
//                IPAddress[] ipAddresses = Dns.GetHostAddresses(hostname);
//                foreach (var ipAddress in ipAddresses)
//                {
//                    TcpClient tcpClient = null;
//                    try
//                    {
//                        tcpClient = new TcpClient(ipAddress.AddressFamily);

//                        // NOTE there was an issue on non-windows platforms.
//                        // non-windows platforms block on synchronous connect, use send/receive timeouts: https://github.com/dotnet/corefx/issues/20954
//                        IAsyncResult result = tcpClient.ConnectAsync(ipAddress, aConnSetting.Port);
//                        var success = result.AsyncWaitHandle.WaitOne(TimeSpan.FromMilliseconds(aConnSetting.ConnectionTimeout));
//                        if (!success)
//                        {
//                            throw new MachException(MachErrorMsg.FAIL_TO_CONNECT_SERVER.FormatInvariant(hostname, aConnSetting.Port));
//                        }

//                        m_state = State.Inited;
//                        m_tcpClient = tcpClient;
//                        m_socket = m_tcpClient.Client;
//                        m_payloadHandler = new ProtocolHandler(new SocketHandler(m_socket), new BufferedByteReader());
//                        break;
//                    }
//                    catch (Exception ex)
//                    {
//                        tcpClient?.Client?.Dispose();
//                        SafeDispose(ref tcpClient);

//                        throw ex;
//                    }
//                }

//                // TODO next hostname?
//            }
//        }
//#endif

        public void PrepareSocket(int aTimeout)
        {
            try
            {
                if (aTimeout > 0)
                    m_payloadHandler.SetTimeout(aTimeout);

                InitProtocol sInitProtocol = new InitProtocol();
                sInitProtocol.Generate("CMI_CNET");
                m_payloadHandler.Request(sInitProtocol);
                m_payloadHandler.ResponseInit();

                m_payloadHandler.UnsetTimeout();

                m_state = State.Connecting;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                // TODO clean();
            }
        }

        public void ConnectUser(ConnectionSettings aCsb, int aTimeout)
        {
            try
            {
                if (aTimeout > 0)
                    m_payloadHandler.SetTimeout(aTimeout);

                ConnectProtocol sConnectProtocol = new ConnectProtocol();
                sConnectProtocol.Generate(aCsb);
                m_payloadHandler.Request(sConnectProtocol);
                Protocol sReturn = m_payloadHandler.Response();

                m_payloadHandler.UnsetTimeout();

                m_state = State.Connected;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                // TODO clean();
            }
        }

        public void PrepareSocket()
        {
            PrepareSocket(0);
        }

        public void Disconnect()
        {
            SafeDispose(ref m_tcpClient);
            SafeDispose(ref m_socket);
        }

        public void Transmit(Protocol aProtocol, int aTimeout)
        {
            if (aTimeout > 0)
                m_payloadHandler.SetTimeout(aTimeout);

            // send and recv within the same Protocol
            m_payloadHandler.Request(aProtocol);
            m_payloadHandler.Response(aProtocol);

            m_payloadHandler.UnsetTimeout();
        }

        public void Transmit(Protocol aProtocol)
        {
            // send and recv within the same Protocol
            m_payloadHandler.Request(aProtocol);
            m_payloadHandler.Response(aProtocol);
        }

        public void Send(Protocol aProtocol)
        {
            m_payloadHandler.Request(aProtocol);
        }

        public void Recv(Protocol aProtocol, int aTimeout)
        {
            if (aTimeout > 0)
                m_payloadHandler.SetTimeout(aTimeout);

            m_payloadHandler.Response(aProtocol);

            m_payloadHandler.UnsetTimeout();
        }

        public void Recv(Protocol aProtocol)
        {
            m_payloadHandler.Response(aProtocol);
        }

        public bool RecvBranch(Protocol aProtocol, Protocol aAlterProtocol, int aTimeout)
        {
            bool sIsPrimaryProtocol = true;
            if (aTimeout > 0)
                m_payloadHandler.SetTimeout(aTimeout);

            if (m_payloadHandler.Response(aProtocol, aAlterProtocol) == aProtocol)
                sIsPrimaryProtocol = true; // primary protocol
            else
                sIsPrimaryProtocol = false; // alternative protocol

            m_payloadHandler.UnsetTimeout();

            return sIsPrimaryProtocol;
        }

        public bool RecvBranch(Protocol aProtocol, Protocol aAlterProtocol)
        {
            if (m_payloadHandler.Response(aProtocol, aAlterProtocol) == aProtocol)
                return true; // alternative protocol
            else
                return false; // alternative protocol
        }

        private static void SafeDispose<T>(ref T disposable)
            where T : class, IDisposable
        {
            if (disposable != null)
            {
                try
                {
                    disposable.Dispose();
                }
                catch (SocketException)
                {
                }
                disposable = null;
            }
        }

        public Socket Socket {
            get => m_socket;
            set
            {
                if (m_state == State.Connected)
                    throw new MachException("This is not connected!");
                
                m_socket = value;
            }
        }



        public State ConnectState { get => m_state; set => m_state = value; }

        ProtocolHandler m_payloadHandler;
    }
}
