using RabbitMQ.Client;
using System.Runtime.Remoting.Channels;
using System.Threading.Tasks;

namespace PBT205_Contact_Tracing
{
    class RabbitMQClient
    {
        private string username;
        private string password;
        public IConnection connection;
        public IChannel channel;

        public RabbitMQClient(string _username, string _password)
        {
            username = _username;
            password = _password;
        }
        /*
            ConnectAync: this function creates a new client connection to the
            RabbitMQ server instance. This function is asynchronous which means
            that the connection thread is always running. If the function was not
            asynchronous, it would block other code from running.

            We keep this connection and thread alive until the application is stopped.
            This will allow the client to continually send/receive messages from the
            server.

            IConnection: new TCP connection specific to the client
            IChannel: new stream channel specific to the client dfsghdfhjd
        */

        public async Task ConnectAsync()
        {
            ConnectionFactory factory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = username,
                Password = password
            };
            connection = await factory.CreateConnectionAsync();
            channel = await connection.CreateChannelAsync();
        }
    }
}
