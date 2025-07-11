using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class QueryClient
{
    static void Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Usage: QueryClient <personIds>");
        }
        // Parse the person ID to query from command-line arguments
        string personId = args[0];
        try
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            // Declare the query and query-response queues
            channel.QueueDeclare("query", false, false, false);
            channel.QueueDeclare("query-response", false, false, false);

            // Set up a consumer to receive the tracker’s response
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine($"[RESULT] {message}");
            };
            channel.BasicConsume("query-response", true, consumer);

            // Send the query (person ID) to the tracker
            var body = Encoding.UTF8.GetBytes(personId);
            channel.BasicPublish("", "query", null, body);

            Console.WriteLine("Waiting for tracker response...");
            Thread.Sleep(2000); // Wait for response before exiting
        }
        catch (rabbitMQ.Client.Exceptions.BrokerUnreachableException)
        {
            Console.WriteLine("RabbitMQ connection failed. Is Docker running?");
        }
        catch
        {
            Console.WriteLine($"Critical Error: {ex.Message}");
        }
    }
}