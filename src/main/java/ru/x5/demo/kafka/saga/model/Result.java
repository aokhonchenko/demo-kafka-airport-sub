package ru.x5.demo.kafka.saga.model;

import ru.x5.demo.kafka.saga.enums.TicketStatus;

public class Result {

    private String author = "airport";
    private Integer ticketId;
    private TicketStatus status;
    private Integer orderId;

    // region g/s

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Integer getTicketId() {
        return ticketId;
    }

    public void setTicketId(Integer ticketId) {
        this.ticketId = ticketId;
    }

    public TicketStatus getStatus() {
        return status;
    }

    public void setStatus(TicketStatus status) {
        this.status = status;
    }

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    // endregion

}
