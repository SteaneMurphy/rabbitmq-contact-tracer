using System.Text;
using RabbitMQ.Client;
using Newtonsoft.Json;
using System.Threading;
using System;

// Defines the structure for position messages sent to the tracker
public class PositionMessage
{
    public string Id { get; set; } = string.Empty; // Unique identifier for the person
    public int X { get; set; }     // X coordinate on the board
    public int Y { get; set; }     // Y coordinate on the board
}

class Person
{
    static void Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: Person <personId> <speedMS> [boardSize]");
        }

        // Parse command-line arguments: person ID, movement speed (ms), optional board size
        string personId = args[0];
        int speedMs = int.Parse(args[1]);
        int boardSize = args.Length > 2 ? int.Parse(args[2]) : 10;

        var rand = new Random();
        // Start at a random position on the board
        int x = rand.Next(boardSize);
        int y = rand.Next(boardSize);

        try
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            // Declare the 'position' queue for publishing positions
            channel.QueueDeclare("position", false, false, false);

            while (true)
            {
                // Create and serialize the position message
                var message = new PositionMessage { Id = personId, X = x, Y = y };
                var json = JsonConvert.SerializeObject(message);
                var body = Encoding.UTF8.GetBytes(json);

                // Publish the position to the 'position' queue
                channel.BasicPublish("", "position", null, body);

                // Randomly move one square in any direction (like a chess king)
                x += rand.Next(-1, 2);
                y += rand.Next(-1, 2);

                // Ensure the new position is within board boundaries
                x = Math.Clamp(x, 0, boardSize - 1);
                y = Math.Clamp(y, 0, boardSize - 1);

                // Wait for the specified movement speed before next move
                Thread.Sleep(speedMs);
            }
        }
        catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException)
        {
            Console.WriteLine("RabbitMQ connection failed. Is Docker running?");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Critical Error: {ex.Message}");
        }
    }
}
