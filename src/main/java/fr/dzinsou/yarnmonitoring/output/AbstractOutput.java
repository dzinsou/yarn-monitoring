package fr.dzinsou.yarnmonitoring.output;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public abstract class AbstractOutput implements Closeable {
    public abstract void bulkSave(List<String> idList, List<String> partitionIdList, List<String> jsonRecordList, boolean async) throws IOException;
}
