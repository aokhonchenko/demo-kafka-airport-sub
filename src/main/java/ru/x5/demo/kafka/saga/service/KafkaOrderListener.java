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
import ru.x5.demo.kafka.saga.dto.OrderUpdateDto;
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

    @KafkaListener(topics = "${app.topic.income-order-topic}", groupId = "airport")
    public void processIncome(@Payload OrderUpdateDto order, Acknowledgment acknowledgment) {

        log.info("Получен новый статус [{}] для заказа {}", order.getStatus(), order.getOrderId());

        if ("pending".equalsIgnoreCase(order.getStatus())) {
            newOrder(order.getOrderId(), acknowledgment);
            return;
        }
        if ("error".equalsIgnoreCase(order.getStatus())) {
            decline(order.getOrderId(), acknowledgment);
            return;
        }
        if ("done".equalsIgnoreCase(order.getStatus())) {
            approve(order.getOrderId(), acknowledgment);
        }
    }

    private void newOrder(String orderId, Acknowledgment acknowledgment) {
        log.info("Получен запрос на авиабилет для заказа {}", orderId);
        Result result = new Result();
        try {

            // lets get some ticket
            Integer ticketId = airTicketService.getNewTicket(orderId);
            result.setOrderId(Integer.getInteger(orderId));
            result.setTicketId(ticketId);

            kafkaTemplate.send(
                    airportProperties.getOutcomeResultTopic(), mapper.writeValueAsString(result));
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            log.error("Отмечаем заказ билета как неудачный. Order = {}", orderId);
            log.error(ex.getMessage(), ex);
            result.setOrderId(Integer.getInteger(orderId));
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

    private void decline(String orderId, Acknowledgment acknowledgment) {
        log.info("Отменяем заказ {}", orderId);
        airTicketService.declineTicket(orderId);
        acknowledgment.acknowledge();
    }

    private void approve(String orderId, Acknowledgment acknowledgment) {
        log.info("Отменяем заказ {}", orderId);
        airTicketService.approveTicket(orderId);
        acknowledgment.acknowledge();
    }
}
