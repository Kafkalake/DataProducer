package com.hooniegit.DataProducer.Kafka;

import com.hooniegit.DataStructure.EDA.Simple.Message;
import com.hooniegit.DataStructure.EDA.Simple.MultiParameter;
import com.hooniegit.DataStructure.EDA.Simple.State;
import com.hooniegit.DataStructure.EDA.Simple.Value;
import com.hooniegit.Xerializer.Kryo.ApachePoolSerializer;
import com.hooniegit.Xerializer.Kryo.PoolSerializer;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Kafka Producer Service
 * - Create Data Instance
 * - Serialize Data (Based on Kryo)
 * - Transmit Data to Kafka Broker
 */
@Service
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;
    private final Random random = new Random();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);
    // 요구사항 중 가장 큰 Parameter Size (Group 5의 2000)
    private static final int MAX_PARAMETER_SIZE = 2000;

    /**
     * Post Construct Task
     */
    @PostConstruct
    private void service() {
        // Spring Boot 초기화 과정이 멈추지 않도록 백그라운드 스레드 생성
        Thread producerThread = new Thread(() -> {
            // 헤더는 반복문 밖에서 한 번만 생성하여 재사용 (객체 생성 최소화)
            Map<String, Object> header = new HashMap<>();
            header.put("local.time", null);
            while (true) {
//            for (int i=0; i<5; i++) {
                long startTime = System.currentTimeMillis();

                // 1. 18만 개의 Message 묶음 생성 및 비동기 전송 작업 트리거
                // (각 워커 스레드가 병렬로 작업을 수행하기 시작합니다)
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                header.replace("local.time", timestamp);
                publishAllGroups(header);
                System.out.println(timestamp);

                // 2. 실행 시간 측정
                long elapsedTime = System.currentTimeMillis() - startTime;

                // 3. 1초(1000ms) 주기 유지를 위한 대기 시간 계산
                long sleepTime = 1000 - elapsedTime;

                if (sleepTime > 0) {
                    try {
                        // 일정한 입력 부하를 유지하기 위해 남은 시간만큼 스레드 대기
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.println("Producer thread was interrupted.");
//                        break;
                    }
                } else {
                    // 데이터 생성 및 스레드 풀 큐잉에 1초가 넘게 걸린 경우 병목 경고 로그
                    System.out.println("Warning: Task scheduling took longer than 1 second (" + elapsedTime + "ms). Check System CPU or Thread Pool Queue.");
                }
            }
        });

        producerThread.start();
    }

    // 각 스레드별로 독립적인 객체 풀(Object Pool)을 유지 (GC 및 False Sharing 원천 차단)
    private final ThreadLocal<ThreadContext> threadContextPool = ThreadLocal.withInitial(() -> {
        Message<List<MultiParameter>> message = new Message<>();
        List<MultiParameter> parameterPool = new ArrayList<>(MAX_PARAMETER_SIZE);

        // 메모리에 미리 할당 (Pre-allocate)
        for (int i = 0; i < MAX_PARAMETER_SIZE; i++) {
            MultiParameter p = new MultiParameter();
            p.setValue(new Value(0.0, 1.0)); // Value 객체도 미리 생성
            parameterPool.add(p);
        }
        return new ThreadContext(message, parameterPool);
    });

    // 외부에서 호출하는 메인 실행 메서드
    public void publishAllGroups(Map<String, Object> header) {
        // 각 그룹을 개별 Task로 고정 스레드 풀에 던집니다.
//        executor.submit(() -> generateAndSend(header, 1, 50_000, 100_000, 2));
//        executor.submit(() -> generateAndSend(header, 50_001, 100_000, 200_000, 5));
//        executor.submit(() -> generateAndSend(header, 150_001, 23_750, 700_000, 6));
//        executor.submit(() -> generateAndSend(header, 173_751, 5_750, 842_500, 10));
//        executor.submit(() -> generateAndSend(header, 179_501, 500, 900_000, 2000));

//        executor.submit(() -> generateAndDeserialize(header, 1, 50_000, 100_000, 2)); // 5만 * 2 = 10만
//        executor.submit(() -> generateAndDeserialize(header, 50_001, 40_000, 200_000, 5)); // 4만 * 5 = 20만

        executor.submit(() -> generateAndSend(header, 1, 50_000, 100_000, 2)); // 5만 * 2 = 10만
        executor.submit(() -> generateAndSend(header, 50_001, 40_000, 200_000, 5)); // 4만 * 5 = 20만

//        executor.submit(() -> generateAndSend(header, 1, 1000, 900_000, 300));
    }

    private void generateAndDeserialize(Map<String, Object> header, int toolStart, int toolSize, int parameterStart, int parameterSize) {
        // 현재 스레드의 재사용 객체 컨텍스트 가져오기
        ThreadContext context = threadContextPool.get();
        Message<List<MultiParameter>> message = context.message;
        List<MultiParameter> pool = context.parameterPool;

//        header.replace("local.time", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        message.setHeader(header);

        for (int tool = toolStart; tool < toolStart + toolSize; tool++) {

//            int partition = tool / 2820;
//            int partition = tool / 15;
            int partition = tool / 1407;

            // 미리 생성된 객체의 '상태값만 변경' (Mutate)
            for (int i = 0; i < parameterSize; i++) {
                int paramId = parameterStart + i;
                MultiParameter p = pool.get(i);

                p.setToolId(tool);
                p.setState(State.PRD);
                p.setStep(null);
                p.setCondition(null);
                p.setId(paramId);

                // Value 객체도 새로 생성하지 않고 내부 값만 업데이트
                p.getValue().setValue(createValue(paramId));
            }

            // 필요한 크기만큼만 subList로 뷰를 생성하여 Message에 세팅 (새 리스트 생성 안 함)
            message.setMessage(new ArrayList<>(pool.subList(0, parameterSize)));

            try {
                byte[] serializedData = PoolSerializer.serialize(message);
                Message<List<MultiParameter>> m = PoolSerializer.deserialize(serializedData);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void generateAndSend(Map<String, Object> header, int toolStart, int toolSize, int parameterStart, int parameterSize) {
        // 현재 스레드의 재사용 객체 컨텍스트 가져오기
        ThreadContext context = threadContextPool.get();
        Message<List<MultiParameter>> message = context.message;
        List<MultiParameter> pool = context.parameterPool;

//        header.replace("local.time", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        message.setHeader(header);

        for (int tool = toolStart; tool < toolStart + toolSize; tool++) {

//            int partition = tool / 2820;
//            int partition = tool / 15;
            int partition = tool / 1407;

            // 미리 생성된 객체의 '상태값만 변경' (Mutate)
            for (int i = 0; i < parameterSize; i++) {
                int paramId = parameterStart + i;
                MultiParameter p = pool.get(i);

                p.setToolId(tool);
                p.setState(State.PRD);
                p.setStep(null);
                p.setCondition(null);
                p.setId(paramId);

                // Value 객체도 새로 생성하지 않고 내부 값만 업데이트
                p.getValue().setValue(createValue(paramId));
            }

            // 필요한 크기만큼만 subList로 뷰를 생성하여 Message에 세팅 (새 리스트 생성 안 함)
            message.setMessage(new ArrayList<>(pool.subList(0, parameterSize)));

            try {
                // [핵심] 직렬화를 '여기서' 수행. Kafka 비동기 전송 시 객체가 덮어씌워지는 것을 방지
                byte[] serializedData = ApachePoolSerializer.serialize(message);
//                byte[] serializedData = PoolSerializer.serialize(message);

                // 직렬화된 바이트 배열 전송 (초당 18만 건이므로 Kafka 배치 설정 튜닝 필수)
                sendMessage("WAT", partition, serializedData);
            } catch (Exception e) {
                System.out.println(e);
                throw new RuntimeException(e);
            }
        }
    }

    private double createValue(int parameter) {
        ThreadLocalRandom random = ThreadLocalRandom.current(); // Lock 경합 방지
        if (parameter % 30000 <= 20000) {
            return random.nextDouble(0.5, 20.0); // origin, bound 순서 주의
        } else {
            return random.nextBoolean() ? 1.0 : 0.0;
        }
    }

    /**
     * Send Message to Kafka
     * @param topic
     * @param partition
     * @param message
     * @throws Exception
     */
    private void sendMessage(String topic, int partition, byte[] message) {
        kafkaTemplate.send(topic, partition, "test", message).whenComplete((result, ex) -> {
            if (ex != null) {
                System.out.println("Failed to send message " + ex);
            }
        });
    }

    // ThreadLocal용 컨테이너 클래스
    private static class ThreadContext {
        final Message<List<MultiParameter>> message;
        final List<MultiParameter> parameterPool;

        ThreadContext(Message<List<MultiParameter>> message, List<MultiParameter> parameterPool) {
            this.message = message;
            this.parameterPool = parameterPool;
        }
    }

}

