using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.IO;

namespace Server
{
    public class Program
    {
        static void Main(string[] args)
        {
            RawSocketServer myServer = new RawSocketServer();
            myServer.Start();
        }
    }
    public class RawSocketServer
    {
        private static Socket listener;
        public static ManualResetEvent allDone = new ManualResetEvent(false);
        public const int _bufferSize = 1024;
        public const int _port = 44444; // listening port
        public static bool _isRunning = true;
        public static List<String> positionList = new List<String>();
        class StateObject
        {
            public Socket workSocket = null;
            public byte[] buffer = new byte[_bufferSize];
            public StringBuilder sb = new StringBuilder();
        }
        
        static bool IsSocketConnected(Socket s)
        {
            return !((s.Poll(1000, SelectMode.SelectRead) && (s.Available == 0)) || !s.Connected);
        }

        public void Start()
        {

            IPEndPoint localEP = new IPEndPoint(IPAddress.Any, _port);
            listener = new Socket(localEP.Address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listener.Bind(localEP);

            while (_isRunning)
            {
                allDone.Reset();
                listener.Listen(20); // The maximum length of the pending connections queue.
                listener.BeginAccept(new AsyncCallback(acceptCallback), listener);
                bool isRequest = allDone.WaitOne(new TimeSpan(12, 0, 0)); // Blocks for 12 hours

                if (!isRequest)
                {
                    allDone.Set();
                }
            }
            listener.Close();
        }

        static void acceptCallback(IAsyncResult ar)
        {
            // Get the listener that handles the client request.
            Socket listener = (Socket)ar.AsyncState;

            if (listener != null)
            {
                Socket handler = listener.EndAccept(ar);

                // Signal main thread to continue
                allDone.Set();

                // Create state
                StateObject state = new StateObject();
                state.workSocket = handler;
                handler.BeginReceive(state.buffer, 0, _bufferSize, 0, new AsyncCallback(readCallback), state);
            }
        }

        static void readCallback(IAsyncResult ar)
        {
            StateObject state = (StateObject)ar.AsyncState;
            Socket handler = state.workSocket;

            if (!IsSocketConnected(handler))
            {
                handler.Close();
                return;
            }

            int read = handler.EndReceive(ar);

            // Data was read from the client socket.
            if (read > 0)
            {
                state.sb.Append(Encoding.UTF8.GetString(state.buffer, 0, read));
                Console.WriteLine(state.sb.ToString());

                String str = state.sb.ToString();
                if (str.IndexOf("*") >= 0 && str.IndexOf("#") >= 0)
                {
                    String[] array = str.Replace("*", "").Replace("#", "").Split(',');
                    String sEpc = array[0];
                    String time = array[1];
                    String android_id = array[2];
                    String ckpt_name = array[3];
                    String position = array[4];
                    
                    byte[] byData = System.Text.Encoding.ASCII.GetBytes(position);
                    handler.Send(byData);
                    handler.Close();
                }
                else
                {
                    handler.BeginReceive(state.buffer, 0, _bufferSize, 0, new AsyncCallback(readCallback), state);
                }
            }
            else
            {
                handler.Close();
            }
        }
    }
}