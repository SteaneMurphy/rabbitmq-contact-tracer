# Distributed Contact Tracer

A distributed contact tracing simulation built in C# using RabbitMQ. Multiple Person processes move independently across a shared grid; a central Tracker detects when two people occupy the same tile and logs the contact. A QueryClient can request the full contact history for any person.

## What's implemented

### Components

**Person** (`Person.cs`)
Simulates a person moving randomly across a grid. Each step, the person publishes its current `(personId, x, y)` position to the `position` queue. Movement uses all eight directions (chess king moves). Configurable via command-line args: `personId`, `speedMs` (move interval), `boardSize`.

**Tracker** (`Tracker.cs`)
Consumes from the `position` queue and maintains two concurrent data structures:
- `ConcurrentDictionary<string, (int x, int y)>` — current position of every known person
- `ConcurrentDictionary<string, List<(string contactId, DateTime time)>>` — full contact log per person

After each position update, the Tracker scans all other known positions. If two people share the same tile, a contact event is recorded for both parties. The Tracker also handles query requests: it reads from the `query` queue and publishes a reverse-chronological contact list to the `query-response` queue.

**QueryClient** (`QueryClient.cs`)
Sends a `personId` to the `query` queue and waits for the response on `query-response`.

### Message flow

```
Person → [position queue] → Tracker (detects collision, logs contact)
QueryClient → [query queue] → Tracker → [query-response queue] → QueryClient
```

## Notes
`Program.cs` is an empty entry point stub and `RabbitMQClient.cs` is a connection wrapper scaffold. The core logic in `Tracker.cs` and `Person.cs` is functional; some type names in `Tracker.cs` use lowercase conventions (draft-quality code).

## Tech stack
- **Language:** C#
- **Messaging:** RabbitMQ (AMQP)
- **Concurrency:** `ConcurrentDictionary` — thread-safe state shared between the position consumer and query handler
