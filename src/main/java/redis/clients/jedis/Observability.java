package redis.clients.jedis;

import io.opencensus.common.Scope;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Aggregation.Distribution;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure;
import io.opencensus.stats.Measure.MeasureDouble;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.stats.View.AggregationWindow.Cumulative;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagContextBuilder;
import io.opencensus.tags.TagValue;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Observability {
    private static final String dimensionless = "1";
    private static final String milliseconds = "ms";

    public static final MeasureLong MBytesRead = MeasureLong.create("redis/bytes_read", "The number of bytes read from the server", dimensionless);
    public static final MeasureLong MBytesWritten = MeasureLong.create("redis/bytes_written", "The number of bytes written to the server", dimensionless);
    public static final MeasureLong MDials = MeasureLong.create("redis/dials", "The number of dials", dimensionless);
    public static final MeasureDouble MDialLatencyMilliseconds = MeasureDouble.create("redis/dial_latency_milliseconds", "The number of milliseconds spent dialling to the Redis server", milliseconds);
    public final MeasureLong MConnectionsTaken = MeasureLong.create("redis/connections_taken", "The number of connections taken", dimensionless);
    public static final MeasureLong MErrors = MeasureLong.create("redis/errors", "The number of errors encountered", dimensionless);
    public static final MeasureLong MConnectionsReturned = MeasureLong.create("redis/connections_returned", "The number of connections returned to the pool", dimensionless);
    public static final MeasureLong MConnectionsReused = MeasureLong.create("redis/connections_reused", "The number of connections reused from to the pool", dimensionless);
    public static final MeasureLong MConnectionsOpened = MeasureLong.create("redis/connections_opened", "The number of opened connections", dimensionless);
    public static final MeasureLong MConnectionsClosed = MeasureLong.create("redis/connections_closed", "The number of closed connections", dimensionless);
    public static final MeasureDouble MRoundtripLatencyMilliseconds = MeasureDouble.create("redis/roundtrip_latency", "The time in milliseconds between sending the first byte to the server until the last byte of response", milliseconds);
    public static final MeasureLong MReads = MeasureLong.create("redis/reads", "The number of read invocations", dimensionless);
    public static final MeasureLong MWrites = MeasureLong.create("redis/writes", "The number of write invocations", dimensionless);

    private static final StatsRecorder statsRecorder = Stats.getStatsRecorder();

    // The respective tags
    public static final TagKey KeyCommandName = TagKey.create("command");
    public static final TagKey KeyEnum = TagKey.create("enum");
    public static final TagKey KeyPhase = TagKey.create("phase");

    private static final Tagger tagger = Tags.getTagger();
    private static Tracer tracer = Tracing.getTracer();

    public static void recordStat(MeasureLong ml, Long n) {
        statsRecorder.newMeasureMap().put(ml, n).record();
    }

    public static void recordStat(MeasureLong ml, int n) {
        statsRecorder.newMeasureMap().put(ml, n).record();
    }

    public static void recordStat(MeasureDouble md, Double d) {
        statsRecorder.newMeasureMap().put(md, d).record();
    }

    public static void recordTaggedStat(TagKey key, String value, MeasureLong ml, int n) {
        recordTaggedStat(key, value, ml, new Long(n));
    }

    public static void recordTaggedStat(TagKey key, String value, MeasureLong ml, Long n) {
        TagContext tctx = tagger.emptyBuilder().put(key, TagValue.create(value)).build();
        Scope ss = tagger.withTagContext(tctx);
        try {
            statsRecorder.newMeasureMap().put(ml, n).record();
        } finally {
            ss.close();
        }
    }

    public static void recordTaggedStat(TagKey key, String value, MeasureDouble md, Double n) {
        TagContext tctx = tagger.emptyBuilder().put(key, TagValue.create(value)).build();
        Scope ss = tagger.withTagContext(tctx);
        try {
            statsRecorder.newMeasureMap().put(md, n).record();
        } finally {
            ss.close();
        }
    }

    public static void recordStatWithTags(MeasureLong ml, long n, TagKeyPair ...pairs) {
        TagContextBuilder tb = tagger.emptyBuilder();

        for (TagKeyPair kvp : pairs) {
            tb.put(kvp.key, TagValue.create(kvp.value));
        }

        // Then finally build it
        TagContext tctx = tb.build();
        Scope ss = tagger.withTagContext(tctx);

        try {
            statsRecorder.newMeasureMap().put(ml, n).record();
        } finally {
            ss.close();
        }
    }

    public static class TagKeyPair {
        private TagKey key;
        private String value;

        public TagKeyPair(TagKey key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static TagKeyPair tagKeyPair(TagKey key, String value) {
        return new TagKeyPair(key, value);
    }

    public static class ScopedSpan implements AutoCloseable {
        public Scope scope;
        public Span span;

        public ScopedSpan(String spanName) {
            this.scope = tracer.spanBuilder(spanName).startScopedSpan();
            this.span  = tracer.getCurrentSpan();
        }

        public void close() {
            this.span.end();
            this.scope.close();
        }

        public void end() {
            this.close();
        }
    }

    public static ScopedSpan createScopedSpan(String spanName) {
        return new ScopedSpan(spanName);
    }

    public static void annotateSpan(Span span, String description, Attribute ...attributes) {
        HashMap<String, AttributeValue> hm = new HashMap<String, AttributeValue>();
        for (Attribute attr : attributes) {
            hm.put(attr.Key, attr.Value);
        }
        span.addAnnotation(Annotation.fromDescriptionAndAttributes(description, hm));
    }

    public static class RoundtripTrackingSpan implements AutoCloseable {
        private Span span;
        private Scope spanScope;
        private long startTimeNs; 
        private String commandName;
        private boolean closed;

        public RoundtripTrackingSpan(String name, String commandName) {
            this.spanScope = tracer.spanBuilder(name).startScopedSpan();
            this.span = tracer.getCurrentSpan();
            this.startTimeNs = System.nanoTime();
            this.commandName = commandName;
        }

        public void end() {
            if (this.closed)
                return;

            long totalTimeNs = System.nanoTime() - this.startTimeNs;
            double timeSpentMilliseconds = (new Double(totalTimeNs))/1e6;
            recordTaggedStat(KeyCommandName, this.commandName, MRoundtripLatencyMilliseconds, timeSpentMilliseconds);
            this.closed = true;
            this.spanScope.close();
        }

        public void close() {
            this.end();
        }
    }

    public static RoundtripTrackingSpan createRoundtripTrackingSpan(String spanName, String commandName) {
        return new RoundtripTrackingSpan(spanName, commandName);
    }

    public static class Attribute {
        private String Key;
        private AttributeValue Value;

        public Attribute(String key, long value) {
            this.Key = key;
            this.Value = AttributeValue.longAttributeValue(value);
        }

        public Attribute(String key, boolean value) {
            this.Key = key;
            this.Value = AttributeValue.booleanAttributeValue(value);
        }
    }

    public static Attribute createAttribute(String key, long value) {
        return new Attribute(key, value);
    }

    public static Attribute createAttribute(String key, boolean value) {
        return new Attribute(key, value);
    }

    public static void registerAllViews() {
        Aggregation defaultBytesDistribution = Distribution.create(BucketBoundaries.create(
                Arrays.asList(
                    // [0, 1KB, 2KB, 4KB, 16KB, 64KB, 256KB,   1MB,     4MB,     16MB,     64MB,     256MB,     1GB,        2GB]
                    0.0, 1024.0, 2048.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0, 67108864.0, 268435456.0, 1073741824.0, 2147483648.0)
                    ));

        Aggregation defaultMillisecondsDistribution =  Distribution.create(BucketBoundaries.create(Arrays.asList(
                        // [0ms, 0.001ms, 0.005ms, 0.01ms, 0.05ms, 0.1ms, 0.5ms, 1ms, 1.5ms, 2ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 200ms, 400ms, 600ms, 800ms, 1s, 1.5s, 2.5s, 5s, 10s, 20s, 40s, 100s, 200s, 500s]
                        0.0, 0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.0015, 0.002, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.5, 2.5, 5.0, 10.0, 20.0, 40.0, 100.0, 200.0, 500.0)));
                
        Aggregation countAggregation = Aggregation.Count.create();
        List<TagKey> noKeys = new ArrayList<TagKey>();

        View[] views = new View[]{
            View.create(
                    Name.create("redis/client/dials"),
                    "The number of dials",
                    MDials,
                    countAggregation,
                    noKeys),

            View.create(
                    Name.create("redis/client/dial_latency"),
                    "The number of milliseconds spent dialling",
                    MDialLatencyMilliseconds,
                    defaultMillisecondsDistribution,
                    noKeys),

            View.create(
                    Name.create("redis/client/connections_opened"),
                    "The number of opened connections",
                    MConnectionsOpened,
                    countAggregation,
                    noKeys),

            View.create(
                    Name.create("redis/client/connections_reused"),
                    "The number of reused connections",
                    MConnectionsReused,
                    countAggregation,
                    noKeys),

            View.create(
                    Name.create("redis/client/connections_closed"),
                    "The number of closed connections",
                    MConnectionsClosed,
                    countAggregation,
                    noKeys),

            View.create(
                    Name.create("redis/client/bytes_read_count"),
                    "The number of bytes read back from the server",
                    MBytesRead,
                    countAggregation,
                    noKeys),

            View.create(
                    Name.create("redis/client/bytes_read_distribution"),
                    "The number of bytes read back from the server",
                    MBytesRead,
                    defaultBytesDistribution,
                    noKeys),

            View.create(
                    Name.create("redis/client/bytes_written_cumulative"),
                    "The number of bytes written to the server",
                    MBytesWritten,
                    countAggregation,
                    noKeys),

            View.create(
                    Name.create("redis/client/bytes_written_distribution"),
                    "The number of bytes written to the server",
                    MBytesWritten,
                    defaultBytesDistribution,
                    noKeys),

            View.create(
                    Name.create("redis/client/roundtrip_latency"),
                    "The distribution of milliseconds",
                    MRoundtripLatencyMilliseconds,
                    defaultMillisecondsDistribution,
                    noKeys),

            View.create(
                    Name.create("redis/client/writes"),
                    "The number of writes",
                    MWrites,
                    countAggregation,
                    noKeys),

            View.create(
                    Name.create("redis/client/reads"),
                    "The number of reads",
                    MReads,
                    countAggregation,
                    noKeys),

            View.create(
                    Name.create("redis/client/errors"),
                    "The number of errors discerned by the various tags",
                    MErrors,
                    countAggregation,
                    Collections.unmodifiableList(Arrays.asList(KeyCommandName, KeyPhase, KeyEnum)))};


        ViewManager vmgr = Stats.getViewManager();

        for (View view : views) {
            vmgr.registerView(view);
        }
    }
}
