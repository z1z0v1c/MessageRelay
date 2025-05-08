import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * A message relay service that processes and forwards messages.
 * <p>
 * TODO for candidate: Implement message rate limiting/throttling to prevent overwhelming
 * the system when a large number of messages arrive in a short period of time.
 */
public class MessageRelay {
    private final BlockingQueue<Message> messageQueue;
    private final Consumer<Message> messageHandler;
    private final Thread processingThread;
    private volatile boolean isRunning;

    /**
     * Constructor for MessageRelay.
     *
     * @param messageHandler The handler that will process forwarded messages
     */
    public MessageRelay(Consumer<Message> messageHandler) {
        this.messageQueue = new LinkedBlockingQueue<>();
        this.messageHandler = messageHandler;
        this.isRunning = true;

        // Start processing thread
        this.processingThread = new Thread(this::processMessages);
        this.processingThread.start();
    }

    /**
     * Receives a message and queues it for processing.
     *
     * @param message The message to be relayed
     */
    public void receiveMessage(Message message) {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        try {
            messageQueue.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to queue message", e);
        }
    }

    /**
     * The main message processing loop.
     * <p>
     * TODO for candidate: This is where you should implement the rate limiting logic.
     * Currently, this method processes messages as fast as they come in, which could
     * cause problems under high load.
     * <p>
     * IMPORTANT: Implement a per-instrument throttling mechanism. Messages for the same
     * instrument should be rate-limited independently of messages for other instruments.
     * Hint: Consider using a Map to track rate limits for each instrument.
     */

    private void processMessages() {
        while (isRunning) {
            try {
                Message message = messageQueue.take();

                // Forward the message to the handler
                messageHandler.accept(message);

                // TODO: Implement rate limiting/throttling here
                // For each instrument, you should limit the rate at which messages can be processed
                // For example, no more than X messages per instrument per second

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (isRunning) {
                    System.err.println("Message processing interrupted: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Stops the message relay service.
     */
    public void shutdown() {
        isRunning = false;
        processingThread.interrupt();
        try {
            processingThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the current size of the message queue.
     *
     * @return The number of pending messages
     */
    public int getPendingMessageCount() {
        return messageQueue.size();
    }

    /**
     * Sample Message class.
     */
    public static class Message {
        private final String id;
        private final String content;
        private final String instrument;
        private final long timestamp;

        public Message(String id, String content, String instrument) {
            this.id = id;
            this.content = content;
            this.instrument = instrument;
            this.timestamp = System.currentTimeMillis();
        }

        public String getId() {
            return id;
        }

        public String getContent() {
            return content;
        }

        public String getInstrument() {
            return instrument;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Message{id='" + id + "', instrument='" + instrument + "', content='" + content + "', timestamp=" + timestamp + '}';
        }
    }
}

