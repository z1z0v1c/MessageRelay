import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * A message relay service that processes and forwards messages with rate limiting/throttling to prevent overwhelming
 * the system when a large number of messages arrive in a short period of time.
 */
public class MessageRelay {
    private final BlockingQueue<Message> messageQueue;
    private final Consumer<Message> messageHandler;
    private final Thread processingThread;
    private final int maxMessages;
    private final long interval;
    private volatile boolean isRunning;

    /**
     * Constructor for MessageRelay.
     *
     * @param messageHandler The handler that will process forwarded messages
     */
    public MessageRelay(Consumer<Message> messageHandler, int maxMessages, long interval) {
        this.messageQueue = new LinkedBlockingQueue<>();
        this.messageHandler = messageHandler;
        this.maxMessages = maxMessages;
        this.interval = interval;
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
     * The main message processing loop with a per-instrument throttling mechanism.
     * Messages for the same instrument are rate-limited independently of messages for other instruments.
     */
    private void processMessages() {
        var timestamps = new HashMap<String, Long>();

        while (isRunning) {
            try {
                Message message = messageQueue.take();

                String instrument = message.getInstrument();
                long now = System.currentTimeMillis();

                // Forward the message to the handler
                if (timestamps.containsKey(instrument)) {
                    long previous = timestamps.get(instrument);
                    if (now - previous >= interval / maxMessages) {
                        messageHandler.accept(message);
                        timestamps.replace(instrument, now);
                    }
                } else {
                    messageHandler.accept(message);
                    timestamps.put(instrument, now);
                }
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
}

