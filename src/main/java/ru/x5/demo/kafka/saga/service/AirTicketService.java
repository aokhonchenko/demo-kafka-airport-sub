package ru.x5.demo.kafka.saga.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.x5.demo.kafka.saga.domain.AirTicket;
import ru.x5.demo.kafka.saga.enums.TicketStatus;
import ru.x5.demo.kafka.saga.exceptions.TicketNotFoundException;
import ru.x5.demo.kafka.saga.repository.AirportTicketRepository;

import java.security.SecureRandom;

@Service
public class AirTicketService {

    private final AirportTicketRepository airportTicketRepository;

    public AirTicketService(AirportTicketRepository airportTicketRepository) {
        this.airportTicketRepository = airportTicketRepository;
    }

    @Transactional
    public Integer getNewTicket(String orderId) {
        // some synthetic errors
        int random = new SecureRandom().nextInt(20);
        if (random == 0) {
            throw new TicketNotFoundException("Не удалось заказать билет");
        }

        AirTicket airTicket = new AirTicket();
        airTicket.setOrderId(orderId);
        airTicket = airportTicketRepository.save(airTicket);
        return airTicket.getId();
    }

    public void declineTicket(String orderId) {
        AirTicket ticket = airportTicketRepository.findByOrderId(orderId);
        ticket.setStatus(TicketStatus.ERROR);
        airportTicketRepository.save(ticket);
    }

    public void approveTicket(String orderId) {
        AirTicket ticket = airportTicketRepository.findByOrderId(orderId);
        ticket.setStatus(TicketStatus.APPROVED);
        airportTicketRepository.save(ticket);
    }
}
