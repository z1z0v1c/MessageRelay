import java.util.UUID;
import java.util.function.Consumer;

/**
 * Main class to test the MessageRelay implementation.
 * This allows candidates to run and test their rate limiting implementation.
 */
class Main {
    public static void main(String[] args) throws InterruptedException {
        // Create a message handler that simply prints the message
        Consumer<Message> messageHandler = message -> {
            System.out.println("Processing message: " + message);

            // Simulate some processing time
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // Create the message relay
        MessageRelay relay = new MessageRelay(messageHandler);

        // List of different instruments to test with
        String[] instruments = {"AAPL", "MSFT", "GOOG", "AMZN", "TSLA"};

        // Simulate a burst of messages with different instruments
        System.out.println("Sending burst of 100 messages across multiple instruments...");
        for (int i = 0; i < 100; i++) {
            String id = UUID.randomUUID().toString();
            String instrument = instruments[i % instruments.length];
            relay.receiveMessage(new Message(id, "Test message " + i, instrument));
        }

        // Wait a bit and check pending messages
        Thread.sleep(1000);
        System.out.println("Pending messages after 1 second: " + relay.getPendingMessageCount());

        // Wait for all messages to be processed
        while (relay.getPendingMessageCount() > 0) {
            System.out.println("Waiting for messages to be processed. Remaining: " + relay.getPendingMessageCount());
            Thread.sleep(1000);
        }

        // Send burst for a single instrument
        System.out.println("\nSending burst of 50 messages for a single instrument (AAPL)...");
        for (int i = 0; i < 50; i++) {
            String id = UUID.randomUUID().toString();
            relay.receiveMessage(new Message(id, "AAPL message " + i, "AAPL"));
        }

        // Wait for some messages to be processed
        Thread.sleep(2000);

        // Send another batch with mixed instruments and delay between messages
        System.out.println("\nSending 20 messages with mixed instruments and delay...");
        for (int i = 0; i < 20; i++) {
            String id = UUID.randomUUID().toString();
            String instrument = instruments[i % instruments.length];
            relay.receiveMessage(new Message(id, "Delayed message " + i, instrument));
            Thread.sleep(100);  // Add delay between messages
        }

        // Wait for all messages to be processed
        while (relay.getPendingMessageCount() > 0) {
            Thread.sleep(100);
        }

        // Shutdown the relay
        System.out.println("\nShutting down message relay...");
        relay.shutdown();
        System.out.println("Test completed.");
    }
}
