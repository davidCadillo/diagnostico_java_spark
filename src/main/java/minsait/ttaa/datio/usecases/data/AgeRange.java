package minsait.ttaa.datio.usecases.data;

public enum AgeRange {

    A(23), B(27), C(32), D(-1);

    private int limit;

    AgeRange(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    public String getRange() {
        return this.toString();
    }
}
