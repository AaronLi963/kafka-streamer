package com.houzz.common;

public class EmailEvent {
    public int teamId;
    public int userId;
    public String attr;

    public EmailEvent(int teamId, int userId, String attr) {
        this.teamId = teamId;
        this.userId = userId;
        this.attr = attr;
    }
}
