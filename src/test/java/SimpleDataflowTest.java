import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class SimpleDataflowTest {

    private static final String STREAM = "stream";
    private static final String PARTITION_KEY = "partition_key";
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true"); //comment out to observe issue
    }

    @Rule
    public final LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack")
            .withTag("0.12.3"))
            .withServices(LocalStackContainer.Service.KINESIS);

    private AmazonKinesis client;

    @Before
    public void before() {
        String endpoint = "http://" + localstack.getHost() + ":" + localstack.getMappedPort(4566);
        String region = localstack.getRegion();
        String accessKey = localstack.getAccessKey();
        String secretKey = localstack.getSecretKey();

        client = AmazonKinesisClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withClientConfiguration(new ClientConfiguration())
                .build();
    }

    @After
    public void after() {
        client.shutdown();
    }

    @Test
    public void test() throws Exception {
        createStream();
        waitForStreamToActivate();

        List<String> messages = IntStream.range(0, 10).boxed()
                .map(i -> "msg_" + i)
                .collect(Collectors.toList());
        sendMessages(messages);

        assertEquals(messages, readMessages());
    }

    private void createStream() {
        System.out.println("Creating stream...");
        client.createStream(STREAM, 1);
        System.out.println("Stream created.");
    }

    public void waitForStreamToActivate() throws Exception {
        while (true) {
            DescribeStreamSummaryRequest request = new DescribeStreamSummaryRequest().withStreamName(STREAM);
            String status = client.describeStreamSummary(request).getStreamDescriptionSummary().getStreamStatus();
            switch (status) {
                case "ACTIVE":
                    return;
                case "CREATING":
                case "UPDATING":
                    System.out.println("Waiting for stream to activate...");
                    TimeUnit.SECONDS.sleep(1);
                    break;
                case "DELETING":
                    throw new RuntimeException("Stream is being deleted");
                default:
                    throw new RuntimeException("Programming error, unhandled stream status: " + status);
            }
        }
    }

    private void sendMessages(List<String> messages) {
        System.out.println("Sending messages...");
        for (String message : messages) {
            byte[] bytes = message.getBytes(CHARSET);
            System.out.println("> " + Arrays.toString(bytes));
            ByteBuffer data = ByteBuffer.wrap(bytes);
            client.putRecord(STREAM, data, PARTITION_KEY);
        }
        System.out.println("Send finished.");
    }

    private List<String> readMessages() {
        System.out.println("Reading messages...");
        List<Shard> shards = client.listShards(new ListShardsRequest().withStreamName(STREAM)).getShards();
        if (shards.size() != 1) {
            throw new RuntimeException("Expected 1 shard, but found " + shards.size() + " in the stream!");
        }

        Shard shard = shards.get(0);
        String shardIteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER.name();
        String startingSequenceNumber = shard.getSequenceNumberRange().getStartingSequenceNumber();
        String shardIterator = client.getShardIterator(STREAM, shard.getShardId(), shardIteratorType, startingSequenceNumber)
                .getShardIterator();
        GetRecordsResult result = client.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));
        System.out.println("Reading messages yielded " + result.getRecords().size() + " results.");
        return result.getRecords().stream()
                .peek(r -> System.out.println("< " + Arrays.toString(r.getData().array())))
                .map(r -> new String(r.getData().array(), CHARSET))
                .collect(Collectors.toList());
    }
}
