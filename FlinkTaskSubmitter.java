//import com.rabbitmq.client.*;
//import okhttp3.*;
//import java.io.*;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class FlinkTaskSubmitter {
//    private static final String RABBITMQ_QUEUE = "task_requests";
//    private static final String RESULT_QUEUE = "task_results";
//    private static final String FLINK_REST_URL = "http://flink-cluster:8081/jars/upload";
//    private static final int MAX_CONCURRENT_SUBMITS = 10;
//    private static final int RESOURCE_CHECK_INTERVAL = 30; // seconds
//
//    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_SUBMITS);
//    private static final BlockingQueue<String> pendingTasks = new LinkedBlockingQueue<>();
//    private static final AtomicInteger retryCounter = new AtomicInteger(0);
//
//    public static void main(String[] args) throws Exception {
//        // 1. 初始化 RabbitMQ
//        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("localhost");
//        Connection connection = factory.newConnection();
//        Channel channel = connection.createChannel();
//        channel.basicQos(MAX_CONCURRENT_SUBMITS);
//        channel.queueDeclare(RABBITMQ_QUEUE, true, false, false, null);
//
//        // 2. 任务提交线程池
//        ExecutorService executor = Executors.newFixedThreadPool(MAX_CONCURRENT_SUBMITS);
//
//        // 3. 消息监听
//        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
//            String taskMessage = new String(delivery.getBody(), "UTF-8");
//            try {
//                pendingTasks.put(taskMessage);
//                executor.submit(() -> processTask(channel, delivery.getEnvelope().getDeliveryTag(), taskMessage));
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        };
//        channel.basicConsume(RABBITMQ_QUEUE, false, deliverCallback, consumerTag -> {});
//
//        // 4. 资源监控线程
//        new Thread(() -> {
//            while (!Thread.currentThread().isInterrupted()) {
//                try {
//                    checkClusterResources();
//                    TimeUnit.SECONDS.sleep(RESOURCE_CHECK_INTERVAL);
//                } catch (InterruptedException e) {
//                    break;
//                }
//            }
//        }).start();
//    }
//
//    private static void processTask(Channel channel, long deliveryTag, String taskMessage) {
//        try {
//            semaphore.acquire();
//            // 检查资源
//            while (!isClusterResourceAvailable()) {
//                TimeUnit.SECONDS.sleep(RESOURCE_CHECK_INTERVAL);
//                pendingTasks.put(taskMessage);
//                return;
//            }
//            // 提交任务到 Flink
//            boolean submitSuccess = submitToFlink(taskMessage);
//            if (submitSuccess) {
//                channel.basicPublish("", RESULT_QUEUE, null, ("Processed: " + taskMessage).getBytes());
//                channel.basicAck(deliveryTag, false);
//            } else {
//                channel.basicNack(deliveryTag, false, true);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            semaphore.release();
//        }
//    }
//
//    // 检查 Flink 集群可用 slot
//    private static boolean isClusterResourceAvailable() throws IOException {
//        OkHttpClient client = new OkHttpClient();
//        Request request = new Request.Builder()
//            .url("http://flink-cluster:8081/taskmanagers")
//            .build();
//        try (Response response = client.newCall(request).execute()) {
//            String responseBody = response.body().string();
//            int availableSlots = parseAvailableSlots(responseBody);
//            return availableSlots > pendingTasks.size() * 2;
//        }
//    }
//
//    // 提交任务到 Flink（实际为上传 JAR 并触发 Neo4jModelRun 逻辑）
//    private static boolean submitToFlink(String taskConfig) {
//        try {
//            // 1. 上传 JAR
//            OkHttpClient client = new OkHttpClient();
//            File jarFile = new File("settingflink.jar");
//            if (!jarFile.exists()) {
//                System.err.println("settingflink.jar not found!");
//                return false;
//            }
//            RequestBody body = new MultipartBody.Builder()
//                .setType(MultipartBody.FORM)
//                .addFormDataPart("jarfile", jarFile.getName(),
//                    RequestBody.create(MediaType.parse("application/java-archive"), jarFile))
//                .build();
//            Request request = new Request.Builder()
//                .url(FLINK_REST_URL)
//                .post(body)
//                .build();
//            String jarId;
//            try (Response response = client.newCall(request).execute()) {
//                if (!response.isSuccessful()) return false;
//                String resp = response.body().string();
//                jarId = parseJarId(resp);
//            }
//
//            // 2. 提交作业（带参数，参数为 taskConfig，实际可传递 Neo4j 算子信息）
//            RequestBody runBody = RequestBody.create(
//                MediaType.parse("application/json"),
//                "{\"entryClass\":\"Neo4jModelRun\",\"programArgs\":\"" + taskConfig + "\"}"
//            );
//            Request runRequest = new Request.Builder()
//                .url("http://flink-cluster:8081/jars/" + jarId + "/run")
//                .post(runBody)
//                .build();
//            try (Response runResponse = client.newCall(runRequest).execute()) {
//                return runResponse.isSuccessful();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    // 解析上传 JAR 后返回的 jarId
//    private static String parseJarId(String jsonResponse) {
//        // 简单实现，实际应用 JSON 解析
//        int idx = jsonResponse.indexOf("jarId");
//        if (idx == -1) return "";
//        int start = jsonResponse.indexOf(":", idx) + 2;
//        int end = jsonResponse.indexOf("\"", start);
//        return jsonResponse.substring(start, end);
//    }
//
//    // 解析可用 slot 数量
//    private static int parseAvailableSlots(String jsonResponse) {
//        // 建议用 JSON 解析库，这里简单处理
//        // 假设返回 {"taskmanagers":[{"slotsNumber":4,"freeSlots":2},...]}
//        int idx = jsonResponse.indexOf("freeSlots");
//        if (idx == -1) return 0;
//        int start = jsonResponse.indexOf(":", idx) + 1;
//        int end = jsonResponse.indexOf(",", start);
//        if (end == -1) end = jsonResponse.indexOf("}", start);
//        return Integer.parseInt(jsonResponse.substring(start, end).trim());
//    }
//
//    // 资源监控（可扩展为 Prometheus 上报等）
//    private static void checkClusterResources() {
//        try {
//            boolean available = isClusterResourceAvailable();
//            System.out.println("Flink cluster available: " + available);
//        } catch (Exception e) {
//            System.err.println("Resource check failed: " + e.getMessage());
//        }
//    }
//}