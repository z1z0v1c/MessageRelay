/**
 * Sample Message class.
 */
public class Message {
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
