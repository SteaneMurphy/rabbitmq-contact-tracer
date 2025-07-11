using system.text;
using system.io;
using system.linq;
using rabbitmq.client;
using rabbitmq.client.events;
using newtonsoft.json;
using system.collections.concurrent; //generic?
using system.xml.schema;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System;

// defines the structure fr position messages received from people
public class positionmessage
{
    public string id { get; set; } // person's unique identifier
    public int x { get; set; } // x coordinate
    public int y { get; set; } // y coordinate
}
class tracker
{
    // maintain position dictionary
    // stores current positions: [personid] => (x, y)
    static concurrentdictionary<string, (int x, int y)> positions = new();

    // stores contact history: [personalid] => list of (contactid, time)
    static concurrentdictionary<string, list<(string contactid, datetime time)>> contactlog = new();

    // centralized error logging
    private static void logerror(string message)
    {
        file.appendalltext("error.log", $"{datetime.now}: {message}\n");
        console.error.writeline($"[error] {message}");
    }

    static void main(string[] args)
    {
        try
        {
            // configure rabbitmq connection
            var factory = new connectionfactory()
            {
                hostname = "localhost",
                username = "guest",
                password = "guest"
            };

            var factory = new connectionfactory { hostname = "locahost" };
            using var connection = factory.createconnectionasync();
            using var channel = connection.createchannelasync();

            // declare the necessary queues
            channel.queuedeclareasync("position", false, false, false);
            channel.queuedeclareasync("query", false, false, false);
            channel.queuedeclareasync("query-response", false, false, false);

            // position consumer: handles movement updates
            var consumer = new asynceventingbasicconsumer(channel);
            consumer.receivedasync += (model, ea) =>
            {
                try
                {
                    // deserialize the incoming posiiton  message
                    var json = encoding.utf8.getstring(ea.body.toarray());
                    var msg = jsonconvert.deserializeobject<positionmessage>(json);

                    // check for collisions with other people,
                    //  check for contacts: has anyone else moved to this square?
                    foreach (var kvp in positions)
                    {
                        if (kvp.key != msg.id && kvp.value == (msg.x, msg.y))
                        {
                            var time = datetime.now;
                            // log contact for both parties
                            contactlog.tryadd(msg.id, new list<(string contactid, datetime time)>());
                            contactlog[msg.id].add((kvp.key, time));
                            contactlog.tryadd(kvp.key, new list<(string contactid, datetime time)>());
                            contactlog[kvp.key].add((msg.id, time));
                        }
                    }
                    // update the person's current position
                    positions[msg.id] = (msg.x, msg.y); // update position

                }
                catch (jsonexception ex)
                {
                    logerror($"json parsing error : {ex.message}");
                }
                catch (exception ex)
                {
                    logerror($"position processing error: {ex.message}");
                }
            };
            channel.bassicconsume("position", true, consumer);

            // query consumer: handles contact queries
            var queryconsumer = new asynceventingbasicconsumer(channel);
            queryconsumer.receivedasync += (model, ea) =>
            {
                try
                {
                    // get the person id being querired
                    var personid = encoding.utf8.getstring((ea.body.toarray()));
                    if (contactlog.trygetvalue(personid, out var contacts))
                    {
                        // prepare a response: list of contacts in reverse-chronological order
                        var response = string.join(", ", contacts.orderbydescending(c => c.time).select(c => $"{c.contactid} at {c.time}"));
                        var responsebody = encoding.utf8.getbytes(response);
                        // publish the response to the "query-reponse' queue.
                        channel.basicpublish("", "query-response", null, responsebody);
                    }
                }
                catch (exception ex)
                {
                    logerror($"query processing error: {ex.message}");
                }
            };
            channel.basicconsume("query", true, consumer);

            console.writeline("tracker running... press [enter] to exit");
            console.readline();
        }
        catch (rabbitmq.client.exceptions.brokerunreachableexception)
        {
            logerror("rabbitmq connection failed. is docker running");

        }
        catch (exception ex)
        {
            logerror($"critical error: {ex.message}");
        }
    }
}