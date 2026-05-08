package company.vk.edu.distrib.compute.goshanchic.consensus;

import java.util.Objects;

public class Message {
    private final MessageType type;
    private final int senderId;
    private final long timestamp;

    public Message(MessageType type, int senderId) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = System.currentTimeMillis();
    }

    public MessageType getType() {
        return type;
    }

    public int getSenderId() {
        return senderId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return senderId == message.senderId && type == message.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, senderId);
    }

    @Override
    public String toString() {
        return "[" + type + " from=" + senderId + "]";
    }
}
