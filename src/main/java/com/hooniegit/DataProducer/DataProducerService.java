package com.hooniegit.DataProducer;

import com.hooniegit.SourceData.Kafka.Body;
import com.hooniegit.SourceData.Kafka.Message;
import com.hooniegit.SourceData.Kafka.State;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataProducerService {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S");

    public Message<List<Body>> generateMessage(int group) {

        // Header 데이터 생성
        HashMap<String, Object> header = new HashMap<>();
        header.put("local.time", LocalDateTime.now().format(formatter));


        // Body 데이터 생성
        List<Body> bodies = new ArrayList<>(2);

        int index_group = group * 2;

        // Facility Property
        int id = index_group + 1;
        State state = State.RUNNING;
        String step = "Sample : Group " + group;
        String condition = "Condition : Group " + group;

        for (int index=0; index<2; index++) {
            int reference = index_group + index + 1;

            // Body Property
            int parameter = reference + 100_000;
            Double value = ((reference % 30000 / 20000) == 0) ? generateAnalogValue() : generateDigitalValue();
            boolean pmmode = (reference % 2 == 0);

            Body body = new Body(parameter, value, pmmode, id, state, step, condition);
            bodies.add(body);
        }

        Message<List<Body>> message = new Message(header, bodies);
        return message;
    }

    public void sendMessage() {
        for (int group=0; group<50000; group++) {
            Message<List<Body>> message = generateMessage(group);
        }
    }

    public Double generateAnalogValue() {
        return Math.random() * 100; // 0.0 ~ 100.0 사이의 랜덤 값
    }

    public Double generateDigitalValue() {
        return Math.random() < 0.5 ? 0.0 : 1.0; // 0.0 또는 1.0 중 랜덤 선택
    }


}
