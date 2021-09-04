package minsait.ttaa.datio.engine.base;

public enum TypeOutput {
    CONSOLE,
    FILE;

    public static TypeOutput getOption(int typeOutput) {
        TypeOutput type;
        if (typeOutput == 1) {
            type = TypeOutput.FILE;
        } else {
            type = TypeOutput.CONSOLE;
        }
        return type;
    }
}

