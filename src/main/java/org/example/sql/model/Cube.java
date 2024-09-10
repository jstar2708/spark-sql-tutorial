package org.example.sql.model;

import java.io.Serializable;

public class Cube implements Serializable {
    int value;

    public int getCube() {
        return cube;
    }

    public void setCube(int cube) {
        this.cube = cube;
    }

    int cube;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public Cube(int value, int cube) {
        this.value = value;
        this.cube = cube;
    }
}
