package dev.faaji.streams.api.v1.domain;

public record UserMatch(String userId, String matchId, int totalCommonInterests) implements Comparable<UserMatch> {

    @Override
    public int compareTo(UserMatch other) {
        if (this.totalCommonInterests() > other.totalCommonInterests()) return -1;
        else if (this.totalCommonInterests() < other.totalCommonInterests()) return 1;
        else {
            return this.userId().compareTo(other.matchId());
        }
    }
}
