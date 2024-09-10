package org.example.sql.model;

import java.io.Serializable;

public class Square implements Serializable {
    public Square(int value, int square) {
        this.value = value;
        this.square = square;
    }

    int value;
    int square;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getSquare() {
        return square;
    }

    public void setSquare(int square) {
        this.square = square;
    }
}
