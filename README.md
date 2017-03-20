## unimqc

A uniform message queue client for Java.

### example

use Aliyun MNS:

```java
public class MNSExample {
    
    public void test() {
        CloudAccount cloudAccount = new CloudAccount(accessKey, secretKey, endpoint);
        MNSClient mnsClient = cloudAccount.getMNSClient();
        
        Codec<Integer, byte[]> codec = new Codec<Integer, byte[]>() {
            @Override
            public byte[] encode(Integer a) {
                return new byte[]{
                        (byte) ((a >> 24) & 0xFF),
                        (byte) ((a >> 16) & 0xFF),
                        (byte) ((a >> 8) & 0xFF),
                        (byte) (a & 0xFF)
                };
            }
   
            @Override
            public Integer decode(byte[] b) {
                return b[3] & 0xFF
                        | (b[2] & 0xFF) << 8
                        | (b[1] & 0xFF) << 16
                        | (b[0] & 0xFF) << 24;
            }
        };
        AliyunMNSClient<Integer> mqClient = new AliyunMNSClient<>(mnsClient, queueName, codec);
        mqClient.init();
        
        // send
        for (int i = 0; i < 1000; ++i) {
            mqClient.send(intval);
        }
        
        // listen
        final int threadNumber = 4;
        final int receiveTimeout = 20;
        final int receiveMaxCount = 10;
        final boolean emptyMessageCallback = false;
        final Consumer<Integer> callback = v -> {
            System.out.println(v);
        };
        try (MQListener mqListener = mqClient.new ListenerBuilder(threadNumber, receiveTimeout, receiveMaxCount, emptyMessageCallback, callback).build()) {
            mqListener.incTask();
            mqListener.incTask();
            mqListener.incTask();
            mqListener.start();
            ExecutorService executorService = mqListener.getExecutorService();
            while (!executorService.isTerminated() && mqListener.getTaskCount() > 0) {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
                mqListener.decTask();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
```