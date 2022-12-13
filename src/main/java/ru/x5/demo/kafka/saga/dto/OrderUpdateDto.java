package ru.x5.demo.kafka.saga.dto;

public class OrderUpdateDto {

    private String orderId;
    private String status;

    // region g/s

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    // endregion

}
