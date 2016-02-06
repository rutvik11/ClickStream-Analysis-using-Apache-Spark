/**
 * Created by rutvikparmar on 05/02/16.
 */
import com.oracle.tools.packager.Log;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;

    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext())
        {
            String location = null;
            String referer = null;
            String sessionId = null;
            String partyId = null;
            String eventType = null;
            try {
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
                System.out.println("raeding message: " + messageAndMetadata.offset());
                GenericRecord genericRecord = byteArrayToDatum(getSchema(), messageAndMetadata.message());
                location = getValue(genericRecord, "locationpage", String.class);
                referer = getValue(genericRecord, "refererpage", String.class);
                sessionId = getValue(genericRecord, "sessionId", String.class);
                partyId = getValue(genericRecord, "partyId", String.class);
                eventType = getValue(genericRecord, "eventType", String.class);


                System.out.println("SessionId :" + sessionId);
                System.out.println("PartyId :" + partyId);
                System.out.println("Referer :" + referer);
                System.out.println("Location :" + location);
                System.out.println("EventType :" + eventType);


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
        //System.out.println("Shutting down Thread: " + m_threadNumber);
    }

    public static GenericRecord byteArrayToDatum(Schema schema, byte[] byteData) {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        ByteArrayInputStream byteArrayInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(byteData);
            Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            return null;
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {

            }
        }
    }

    public static Schema getSchema() {
        String schemaStr = "{\"namespace\": \"io.divolte.examples.record\",\n" +
                "\"type\": \"record\",\n" + "\"name\": \"Record\",\n" +
                "\"fields\": [\n" +
                "{ \"name\": \"sessionId\",     \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "{ \"name\": \"refererpage\",   \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "{ \"name\": \"locationpage\",  \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "{ \"name\": \"eventType\",     \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "{ \"name\": \"partyId\",       \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "\n" +
                "]\n}";
        return new Schema.Parser().parse(schemaStr);
    }

    public static <T> T getValue(GenericRecord genericRecord, String name, Class<T> clazz) {
        Object obj = genericRecord.get(name);
        if (obj == null)
            return null;
        if (obj.getClass() == Utf8.class) {
            return (T) obj.toString();
        }
        if (obj.getClass() == Integer.class) {
            return (T) obj;
        }
        return null;
    }
}
