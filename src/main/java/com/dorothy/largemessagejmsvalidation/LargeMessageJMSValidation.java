package com.dorothy.largemessagejmsvalidation;

import com.google.gson.reflect.TypeToken;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;


public class LargeMessageJMSValidation {
    static final String SB_SAMPLES_CONNECTIONSTRING =
            "Endpoint=sb://logicpremium.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mkXO5B/4AeFTvOecY2HUYl+625QIHOPoaY6Ipv2Jktc=";

    static String queueName = "BasicQueue";
    static String topicName = "BasicTopic";
    static String subName = "Sub1";

    static void registerReceiver(QueueClient queueClient, ExecutorService executorService) throws Exception {
        queueClient.registerMessageHandler(new IMessageHandler() {
                                               // callback invoked when the message handler loop has obtained a message
                                               public CompletableFuture<Void> onMessageAsync(IMessage message) {
                                                   System.out.printf(
                                                           "\n\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = %s, \n\t\t\t\t\t\tSequenceNumber = %s, \n\t\t\t\t\t\tEnqueuedTimeUtc = %s," +
                                                                   "\n\t\t\t\t\t\tExpiresAtUtc = %s, \n\t\t\t\t\t\tContentType = \"%s\"\n",
                                                           message.getMessageId(),
                                                           message.getSequenceNumber(),
                                                           message.getEnqueuedTimeUtc(),
                                                           message.getExpiresAtUtc(),
                                                           message.getContentType());
                                                   return CompletableFuture.completedFuture(null);
                                               }

                                               // callback invoked when the message handler has an exception to report
                                               public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                                                   System.out.printf(exceptionPhase + "-" + throwable.getMessage());
                                               }
                                           },
                // 1 concurrent call, messages are auto-completed, auto-renew duration
                new MessageHandlerOptions(1, true, Duration.ofMinutes(3)),
                executorService);
    }

    private static String createDataSize(int msgSize) {
//        StringBuilder sb = new StringBuilder(msgSize);
//        for (int i=0; i<msgSize; i++) {
//            sb.append('a');
//        }
//        return sb.toString();
        char[] chars = new char[msgSize];
        Arrays.fill(chars, 'a');

        String str = new String(chars);
        return str;
    }

    static CompletableFuture<Void> sendMessagesAsync(QueueClient sendClient) {
        List<CompletableFuture> tasks = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            final String messageId = Integer.toString(i);
            try{
                // message size is 100mb 100000000 / 2
                // 64000000 / 2
                // 50000000 / 2
                // 32000000 / 2
                // 16000000 / 2
                // 8000000 / 2
                //byte[] content = createDataSize(4000000 / 2).getBytes(UTF_8);
                //System.out.printf("\n\tMessage byte size: %d", content.length);
                Message message = new Message(createDataSize(4000000 / 2));
                message.setMessageId(messageId);
                //message.setTimeToLive(Duration.ofMinutes(2));
                //message.setContentType("application/json");
                //message.setLabel("Scientist");
                System.out.printf("\nMessage sending: Id = %s", message.getMessageId());
                tasks.add(
                        sendClient.sendAsync(message).thenRunAsync(() -> {
                            System.out.printf("\n\tMessage acknowledged: Id = %s", message.getMessageId());
                        }));
            } catch (Exception e){
                System.out.println(e.toString());
            }
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[tasks.size()]));
    }

    static void QueueTest() throws Exception{
//        QueueClient receiveClient = new QueueClient(new ConnectionStringBuilder(SB_SAMPLES_CONNECTIONSTRING, queueName), ReceiveMode.PEEKLOCK);
//        // We are using single thread executor as we are only processing one message at a time
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        registerReceiver(receiveClient, executorService);

        // Create a QueueClient instance for sending and then asynchronously send messages.
        // Close the sender once the send operation is complete.
        QueueClient sendClient = new QueueClient(new ConnectionStringBuilder(SB_SAMPLES_CONNECTIONSTRING, queueName), ReceiveMode.PEEKLOCK);
        sendMessagesAsync(sendClient).thenRunAsync(
                () -> sendClient.closeAsync());

        waitForEnter(500);
    }

    static private void waitForEnter(int seconds) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            executor.invokeAny(Arrays.asList(() -> {
                System.in.read();
                return 0;
            }, () -> {
                Thread.sleep(seconds * 1000);
                return 0;
            }));
        } catch (Exception e) {
            // absorb
        }
    }

    public static void main(String[] args) throws Exception {
        QueueTest();
    }
}
