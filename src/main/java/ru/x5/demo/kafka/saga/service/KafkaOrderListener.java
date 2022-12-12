package ru.x5.demo.kafka.saga.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import ru.x5.demo.kafka.saga.config.AirportProperties;
import ru.x5.demo.kafka.saga.enums.TicketStatus;
import ru.x5.demo.kafka.saga.model.Result;

@Service
public class KafkaOrderListener {

    private final Logger log = LoggerFactory.getLogger(KafkaOrderListener.class);

    private final AirTicketService airTicketService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private final AirportProperties airportProperties;

    private final ObjectMapper mapper;

    public KafkaOrderListener(
            AirTicketService airTicketService,
            KafkaTemplate<String, String> kafkaTemplate,
            AirportProperties airportProperties,
            ObjectMapper mapper) {
        this.airTicketService = airTicketService;
        this.kafkaTemplate = kafkaTemplate;
        this.airportProperties = airportProperties;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "order", groupId = "airport")
    public void processIncome(@Payload String order, Acknowledgment acknowledgment) {

        log.info("Получен запрос на авиабилет для заказа {}", order);
        Result result = new Result();
        try {

            // lets get some ticket
            Integer ticketId = airTicketService.getNewTicket();
            result.setOrderId(Integer.getInteger(order));
            result.setTicketId(ticketId);

            kafkaTemplate.send(
                    airportProperties.getOutcomeResultTopic(), mapper.writeValueAsString(result));
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            log.error("Отмечаем заказ билета как неудачный. Order = {}", order);
            log.error(ex.getMessage(), ex);
            result.setOrderId(Integer.getInteger(order));
            result.setStatus(TicketStatus.ERROR);
            try {
                kafkaTemplate.send(
                        airportProperties.getOutcomeResultTopic(),
                        mapper.writeValueAsString(result));
            } catch (JsonProcessingException ignore) {
                // don't do it in production. Let's mark this exception as impossible just here.
            }
        } finally {
            acknowledgment.acknowledge();
        }
    }
}
