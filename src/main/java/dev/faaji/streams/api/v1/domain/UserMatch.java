package dev.faaji.streams.api.v1.domain;

import java.util.Objects;

public record UserMatch(String userId, String matchId, int totalCommonInterests) implements Comparable<UserMatch> {

    @Override
    public int compareTo(UserMatch other) {
        if (this.totalCommonInterests() > other.totalCommonInterests()) return -1;
        else if (this.totalCommonInterests() < other.totalCommonInterests()) return 1;
        else {
            return this.userId().compareTo(other.userId());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserMatch userMatch = (UserMatch) o;
        return Objects.equals(userId, userMatch.userId) && Objects.equals(matchId, userMatch.matchId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, matchId);
    }
}
