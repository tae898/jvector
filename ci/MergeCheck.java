// UNCOMPILED (written 2026-11-03 against the public API of datastax/jvector PR #777 head 7fd3e46708; first compile happens in the container).
// Our own merge check for the three things ArcadeDB needs from OnDiskGraphIndexCompactor:
//   1. topology-only source graphs (InlineVectors of dimension 0) + vectors from the caller (RandomAccessVectorValues),
//   2. a file-backed IndexWriter as the output (compact(IndexWriter, Path)),
//   3. caller-chosen output ordinals (withOutputOrdinals), here "rank of a global vector id among the live nodes".
// Two graphs (default 20000 and 5000 unit vectors, 128-d, cosine), a fraction of nodes deleted via liveNodes.
// It prints RESULT lines (also appended to $MC_RESULT if set) and fails only on hard invariants (size, ordinal mapping).
// Run as a JUnit4 test inside jvector-tests (see build_test.sh, MODE=mergecheck) so the module classpath and --add-modules apply;
// the main() is the same code. Public API only: nothing from the PR's test helpers is used.
package mergecheck;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.disk.SimpleWriter;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

public class MergeCheck {
    private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final VectorSimilarityFunction VSF = VectorSimilarityFunction.COSINE;
    private static final int PREFIX = 13; // odd start offset on purpose: the output must not assume it starts at 0

    static int envInt(String k, int d) { String v = System.getenv(k); return v == null || v.isEmpty() ? d : Integer.parseInt(v); }
    static double envDouble(String k, double d) { String v = System.getenv(k); return v == null || v.isEmpty() ? d : Double.parseDouble(v); }
    static boolean envBool(String k, boolean d) { String v = System.getenv(k); return v == null || v.isEmpty() ? d : Boolean.parseBoolean(v); }

    static PrintOut out;

    static final class PrintOut {
        final Path file;
        PrintOut(String f) { file = f == null || f.isEmpty() ? null : Path.of(f); }
        void result(String s) throws IOException {
            String line = "RESULT " + s;
            System.out.println(line);
            if (file != null) Files.writeString(file, line + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    @org.junit.Test
    public void run() throws Exception {
        main(new String[0]);
    }

    public static void main(String[] args) throws Exception {
        int na = envInt("MC_NA", 20000), nb = envInt("MC_NB", 5000), dim = envInt("MC_DIM", 128);
        double del = envDouble("MC_DEL", 0.15);
        int queries = envInt("MC_Q", 200), k = envInt("MC_K", 10), seed = envInt("MC_SEED", 42);
        int m = envInt("MC_M", 32), beam = envInt("MC_BEAM", 100);
        boolean hier = envBool("MC_HIER", false); // the PR's tests build with addHierarchy=false
        int[] efs = Arrays.stream(System.getenv().getOrDefault("MC_EFS", "50,100,200").split(",")).mapToInt(Integer::parseInt).toArray();
        out = new PrintOut(System.getenv("MC_RESULT"));
        Path dir = Files.createTempDirectory("jvector777_mergecheck");
        Path work = Files.createDirectory(dir.resolve("work"));
        ForkJoinPool pool = new ForkJoinPool(Math.max(2, Runtime.getRuntime().availableProcessors()));
        List<ReaderSupplier> readers = new ArrayList<>();
        try {
            out.result("config java=" + System.getProperty("java.version") + " na=" + na + " nb=" + nb + " dim=" + dim
                       + " del=" + del + " k=" + k + " queries=" + queries + " M=" + m + " beam=" + beam + " hier=" + hier
                       + " cpus=" + Runtime.getRuntime().availableProcessors() + " vsf=COSINE seed=" + seed);
            Random rnd = new Random(seed);
            int[] sizes = {na, nb};
            int S = sizes.length;
            List<List<VectorFloat<?>>> vecs = new ArrayList<>();
            for (int n : sizes) vecs.add(randomUnit(rnd, n, dim));

            // 1. two topology-only source graphs on disk
            List<OnDiskGraphIndex> sources = new ArrayList<>();
            List<RandomAccessVectorValues> ravvs = new ArrayList<>();
            for (int s = 0; s < S; s++) {
                long t0 = System.nanoTime();
                RandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vecs.get(s), dim);
                ravvs.add(ravv);
                Path p = dir.resolve("source_" + s);
                writeTopologyOnly(p, ravv, dim, m, beam, hier, pool);
                ReaderSupplier rs = ReaderSupplierFactory.open(p);
                readers.add(rs);
                OnDiskGraphIndex g = OnDiskGraphIndex.load(rs);
                sources.add(g);
                out.result("source" + s + " nodes=" + sizes[s] + " stored_dimension=" + g.getDimension() + " file_bytes=" + Files.size(p)
                           + " build_write_seconds=" + String.format("%.2f", (System.nanoTime() - t0) / 1e9));
                if (g.getDimension() != 0) throw new AssertionError("source " + s + " is not topology-only");
            }

            // 2. deletions (liveNodes) and caller-chosen output ordinals: rank of a random global id among the live nodes
            List<FixedBitSet> live = new ArrayList<>();
            int total = 0, liveCount = 0;
            for (int s = 0; s < S; s++) {
                FixedBitSet bits = new FixedBitSet(sizes[s]);
                for (int i = 0; i < sizes[s]; i++) if (rnd.nextDouble() >= del) bits.set(i);
                live.add(bits);
                total += sizes[s];
                liveCount += bits.cardinality();
            }
            int[] perm = new int[total];
            for (int i = 0; i < total; i++) perm[i] = i;
            for (int i = total - 1; i > 0; i--) { int j = rnd.nextInt(i + 1); int t = perm[i]; perm[i] = perm[j]; perm[j] = t; }
            // key = global id; ids of live nodes sorted ascending -> output ordinal = rank
            List<int[]> ids = new ArrayList<>();
            int off = 0;
            for (int s = 0; s < S; s++) { ids.add(Arrays.copyOfRange(perm, off, off + sizes[s])); off += sizes[s]; }
            int[] nodeOfKey = new int[total];
            int[] srcOfKey = new int[total];
            for (int s = 0; s < S; s++) for (int i = 0; i < sizes[s]; i++) { nodeOfKey[ids.get(s)[i]] = i; srcOfKey[ids.get(s)[i]] = s; }
            List<int[]> outputOrdinals = new ArrayList<>();
            for (int s = 0; s < S; s++) { int[] a = new int[sizes[s]]; Arrays.fill(a, -1); outputOrdinals.add(a); }
            int rank = 0;
            for (int id = 0; id < total; id++) {
                int s = srcOfKey[id], node = nodeOfKey[id];
                if (live.get(s).get(node)) outputOrdinals.get(s)[node] = rank++;
            }
            if (rank != liveCount) throw new AssertionError("rank " + rank + " != live " + liveCount);
            out.result("live nodes=" + liveCount + " deleted=" + (total - liveCount));

            // 3. merge through the new constructor into a file-backed IndexWriter, with the caller's ordinals
            Path mergedPath = dir.resolve("merged");
            OnDiskGraphIndexCompactor compactor = new OnDiskGraphIndexCompactor(sources, ravvs, null, live, VSF, pool)
                    .withOutputOrdinals(outputOrdinals);
            long startPos;
            final long[] peakWork = {0};
            Thread sampler = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try (var st = Files.walk(work)) {
                        long sz = st.filter(Files::isRegularFile).mapToLong(x -> { try { return Files.size(x); } catch (IOException e) { return 0L; } }).sum();
                        if (sz > peakWork[0]) peakWork[0] = sz;
                        Thread.sleep(50);
                    } catch (IOException e) {
                        // work dir briefly unreadable: try again
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, "work-dir-sampler");
            sampler.setDaemon(true);
            sampler.start();
            long t0 = System.nanoTime();
            try (SimpleWriter w = new SimpleWriter(mergedPath)) {
                w.write(new byte[PREFIX]);
                startPos = w.position();
                compactor.compact(w, work);
            }
            double mergeSeconds = (System.nanoTime() - t0) / 1e9;
            sampler.interrupt();
            sampler.join();
            long workLeft;
            try (var st = Files.list(work)) { workLeft = st.count(); }
            ReaderSupplier mrs = ReaderSupplierFactory.open(mergedPath);
            readers.add(mrs);
            OnDiskGraphIndex merged = OnDiskGraphIndex.load(mrs, startPos);
            out.result("merge_seconds=" + String.format("%.2f", mergeSeconds) + " output_bytes=" + (Files.size(mergedPath) - startPos)
                       + " work_dir_peak_bytes_sampled=" + peakWork[0] + " work_dir_files_left=" + workLeft + " merged_stored_dimension=" + merged.getDimension());

            // checks: node count, ordinal mapping, deleted not present
            int outputNodes = merged.size(0);
            int mapperMismatch = 0;
            List<OrdinalMapper> mappers = compactor.ordinalMappers();
            for (int s = 0; s < S; s++)
                for (int i = 0; i < sizes[s]; i++)
                    if (live.get(s).get(i) && mappers.get(s).oldToNew(i) != outputOrdinals.get(s)[i]) mapperMismatch++;
            int missing = 0;
            for (int o = 0; o < liveCount; o++) if (!merged.containsNode(o)) missing++;
            boolean exactCount = outputNodes == liveCount && merged.getIdUpperBound() == liveCount && missing == 0;
            out.result("output_nodes=" + outputNodes + " expected_live=" + liveCount + " id_upper_bound=" + merged.getIdUpperBound()
                       + " missing_ordinals=" + missing + " mapper_mismatch=" + mapperMismatch + " deleted_not_present=" + exactCount);
            if (!exactCount || mapperMismatch != 0) throw new AssertionError("merged graph does not hold exactly the live nodes at the chosen ordinals");

            // 4. recall@k vs exact over the live vectors, by output ordinal
            List<VectorFloat<?>> byOrdinal = new ArrayList<>(Collections.nCopies(liveCount, (VectorFloat<?>) null));
            for (int s = 0; s < S; s++)
                for (int i = 0; i < sizes[s]; i++)
                    if (live.get(s).get(i)) byOrdinal.set(outputOrdinals.get(s)[i], vecs.get(s).get(i));
            RandomAccessVectorValues mergedRavv = new ListRandomAccessVectorValues(byOrdinal, dim);
            List<VectorFloat<?>> qs = randomUnit(new Random(seed + 1L), queries, dim);
            int[][] truth = new int[queries][];
            for (int q = 0; q < queries; q++) truth[q] = exactTopK(qs.get(q), byOrdinal, k);

            for (int ef : efs) {
                // merged graph
                long hitsMerged = 0;
                long tq = System.nanoTime();
                try (GraphSearcher searcher = new GraphSearcher(merged)) {
                    for (int q = 0; q < queries; q++) {
                        var ssp = DefaultSearchScoreProvider.exact(qs.get(q), VSF, mergedRavv);
                        SearchResult r = searcher.search(ssp, k, ef, 0.0f, 0.0f, Bits.ALL);
                        hitsMerged += overlap(truth[q], Arrays.stream(r.getNodes()).mapToInt(ns -> ns.node).toArray());
                    }
                }
                double mergedMs = (System.nanoTime() - tq) / 1e6 / queries;
                // reference: each source searched separately (live nodes only), top-k merged by score
                long hitsSeparate = 0;
                tq = System.nanoTime();
                List<GraphSearcher> searchers = new ArrayList<>();
                try {
                    for (int s = 0; s < S; s++) searchers.add(new GraphSearcher(sources.get(s)));
                    for (int q = 0; q < queries; q++) {
                        List<float[]> cand = new ArrayList<>(); // {score, outputOrdinal}
                        for (int s = 0; s < S; s++) {
                            var ssp = DefaultSearchScoreProvider.exact(qs.get(q), VSF, ravvs.get(s));
                            SearchResult r = searchers.get(s).search(ssp, k, ef, 0.0f, 0.0f, live.get(s));
                            for (var ns : r.getNodes()) cand.add(new float[]{ns.score, outputOrdinals.get(s)[ns.node]});
                        }
                        cand.sort((x, y) -> Float.compare(y[0], x[0]));
                        int[] got = new int[Math.min(k, cand.size())];
                        for (int i = 0; i < got.length; i++) got[i] = (int) cand.get(i)[1];
                        hitsSeparate += overlap(truth[q], got);
                    }
                } finally {
                    for (GraphSearcher gs : searchers) gs.close();
                }
                double sepMs = (System.nanoTime() - tq) / 1e6 / queries;
                double denom = (double) queries * k;
                out.result("recall ef=" + ef + " k=" + k + " merged=" + String.format("%.4f", hitsMerged / denom)
                           + " separate_sources_topk_merge=" + String.format("%.4f", hitsSeparate / denom)
                           + " merged_ms_per_query=" + String.format("%.3f", mergedMs) + " separate_ms_per_query=" + String.format("%.3f", sepMs));
            }
            out.result("done");
        } finally {
            for (ReaderSupplier rs : readers) { try { rs.close(); } catch (Exception ignored) { } }
            pool.shutdown();
            deleteRecursively(dir);
        }
    }

    static List<VectorFloat<?>> randomUnit(Random rnd, int n, int dim) {
        List<VectorFloat<?>> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            VectorFloat<?> v = VTS.createFloatVector(dim);
            for (int d = 0; d < dim; d++) v.set(d, (float) rnd.nextGaussian());
            VectorUtil.l2normalize(v);
            l.add(v);
        }
        return l;
    }

    /** Writes a topology-only graph: the same writer calls the PR's CompactionSources uses, InlineVectors of dimension 0. */
    static void writeTopologyOnly(Path path, RandomAccessVectorValues ravv, int dim, int m, int beam, boolean hier, ForkJoinPool pool) throws IOException {
        var builder = new GraphIndexBuilder(BuildScoreProvider.randomAccessScoreProvider(ravv, VSF), dim, m, beam, 1.2f, 1.2f, hier, true, pool, pool);
        IntStream.range(0, ravv.size()).parallel().forEach(i -> builder.addGraphNode(i, ravv.getVector(i)));
        builder.cleanup();
        var graph = builder.getGraph();
        VectorFloat<?> empty = VTS.createFloatVector(0);
        Map<FeatureId, IntFunction<Feature.State>> suppliers = new EnumMap<>(FeatureId.class);
        suppliers.put(FeatureId.INLINE_VECTORS, ord -> new InlineVectors.State(empty));
        try (var writer = new OnDiskGraphIndexWriter.Builder(graph, path)
                .withMapper(new OrdinalMapper.IdentityMapper(ravv.size() - 1))
                .with(new InlineVectors(0))
                .build()) {
            writer.write(suppliers);
        }
    }

    static int[] exactTopK(VectorFloat<?> q, List<VectorFloat<?>> vs, int k) {
        float[] sc = new float[vs.size()];
        Integer[] idx = new Integer[vs.size()];
        for (int i = 0; i < sc.length; i++) { sc[i] = VSF.compare(q, vs.get(i)); idx[i] = i; }
        Arrays.sort(idx, (a, b) -> Float.compare(sc[b], sc[a]));
        int[] top = new int[Math.min(k, idx.length)];
        for (int i = 0; i < top.length; i++) top[i] = idx[i];
        return top;
    }

    static int overlap(int[] truth, int[] got) {
        Set<Integer> t = new HashSet<>();
        for (int x : truth) t.add(x);
        int h = 0;
        for (int x : got) if (t.contains(x)) h++;
        return h;
    }

    static void deleteRecursively(Path p) {
        try (var st = Files.walk(p)) {
            st.sorted(Collections.reverseOrder()).forEach(x -> { try { Files.deleteIfExists(x); } catch (IOException ignored) { } });
        } catch (IOException ignored) { }
    }
}
