package restore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/perfmark"
)

// memoryCacheVersion is bumped whenever the on-disk layout or semantics of a
// materialized layer changes; an unknown version is treated as a cache miss.
const memoryCacheVersion = 1

// MemoryCache materializes immutable manifest-backed memory layers into local
// raw sparse files and serves restore faults from them.
//
// The cache is a pure derived view of content-addressed artifacts: entries are
// keyed by (ref, size), can be deleted at any time, and are never authoritative.
// Only Data runs are written; Hole/Zero runs live in the sidecar and read as
// zeros, preserving the layered source's sparse semantics.
type MemoryCache struct {
	dir      string
	maxBytes int64
	logf     func(string, ...any)
}

func newMemoryCache(dir string, maxBytes int64, logf func(string, ...any)) *MemoryCache {
	return &MemoryCache{dir: dir, maxBytes: maxBytes, logf: logf}
}

// memRun mirrors one sparse run of the original memory layer.
type memRun struct {
	Off  uint64 `json:"off"`
	End  uint64 `json:"end"`
	Kind int    `json:"kind"`
}

type memCacheMeta struct {
	Version int      `json:"version"`
	Ref     string   `json:"ref"`
	Size    uint64   `json:"size"`
	Runs    []memRun `json:"runs"`
}

// wrap returns a stream backed by the local cache when ref is a manifest
// memory layer and the cache is usable. The boolean reports whether the caller
// owns a newly opened cache stream and must Close it. Any cache failure falls
// back to src so caching can never fail a restore.
func (c *MemoryCache) wrap(ctx context.Context, p *perfmark.Marks, ref string, src fetch.Stream) (fetch.Stream, bool) {
	if c == nil || c.dir == "" || src == nil || ref == "" {
		return src, false
	}
	// A file:// layer is already a direct local read; duplicating it into the
	// cache only adds I/O. Only content-addressed manifest layers benefit.
	if !strings.HasPrefix(ref, "manifest://") {
		return src, false
	}
	size := src.Size()
	if size == 0 {
		return src, false
	}
	key := memoryCacheKey(ref, size)
	rawPath := filepath.Join(c.dir, key+".raw")
	metaPath := filepath.Join(c.dir, key+".runs")
	if stream, ok := c.open(rawPath, metaPath, ref, size); ok {
		p.MarkKV("restore.memory_cache.hit", map[string]any{"ref": ref, "size": size})
		c.logf("memory cache hit %s (%d bytes)", rawPath, size)
		return stream, true
	}

	start := time.Now()
	runs, err := c.materialize(ctx, src, size, rawPath)
	if err != nil {
		p.MarkKV("restore.memory_cache.error", map[string]any{"ref": ref, "err": err.Error()})
		c.logf("memory cache materialize %s: %v; using source", rawPath, err)
		return src, false
	}
	if err := writeMeta(metaPath, memCacheMeta{Version: memoryCacheVersion, Ref: ref, Size: size, Runs: runs}); err != nil {
		p.MarkKV("restore.memory_cache.error", map[string]any{"ref": ref, "err": err.Error()})
		c.logf("memory cache metadata %s: %v; using source", metaPath, err)
		return src, false
	}
	if evicted, freed := c.evict(rawPath); evicted > 0 {
		p.MarkKV("restore.memory_cache.evict", map[string]any{"entries": evicted, "bytes": freed})
		c.logf("memory cache evicted %d entries (%d bytes)", evicted, freed)
	}
	p.MarkKV("restore.memory_cache.materialize", map[string]any{
		"ref": ref, "size": size, "runs": len(runs),
	})
	p.Dur("restore.memory_cache.materialize_ms", start)
	c.logf("memory cache materialized %s (%d bytes, %d runs)", rawPath, size, len(runs))

	stream, ok := c.open(rawPath, metaPath, ref, size)
	if !ok {
		return src, false
	}
	return stream, true
}

func memoryCacheKey(ref string, size uint64) string {
	sum := sha256.Sum256([]byte(ref + "\x00" + strconv.FormatUint(size, 10)))
	return hex.EncodeToString(sum[:])
}

// open returns a raw-file stream only when both the file and a valid sidecar
// exist. A crash between the two renames therefore degrades to a cache miss.
func (c *MemoryCache) open(rawPath, metaPath, ref string, size uint64) (fetch.Stream, bool) {
	meta, err := readMeta(metaPath)
	if err != nil || meta.Version != memoryCacheVersion || meta.Ref != ref || meta.Size != size {
		return nil, false
	}
	if err := validateRuns(meta.Runs, size); err != nil {
		return nil, false
	}
	info, err := os.Stat(rawPath)
	if err != nil || info.Size() != int64(size) {
		return nil, false
	}
	f, err := os.Open(rawPath)
	if err != nil {
		return nil, false
	}
	return &rawFileStream{f: f, size: size, runs: meta.Runs}, true
}

func readMeta(path string) (memCacheMeta, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return memCacheMeta{}, err
	}
	var meta memCacheMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return memCacheMeta{}, err
	}
	return meta, nil
}

func writeMeta(path string, meta memCacheMeta) error {
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

func validateRuns(runs []memRun, size uint64) error {
	if len(runs) == 0 {
		return errors.New("empty run map")
	}
	var next uint64
	for _, run := range runs {
		if run.Off != next || run.End <= run.Off || run.End > size {
			return errors.New("run map is not a contiguous cover")
		}
		if run.Kind < int(sparse.Hole) || run.Kind > int(sparse.Data) {
			return errors.New("run map has an unknown kind")
		}
		next = run.End
	}
	if next != size {
		return errors.New("run map does not cover the layer")
	}
	return nil
}

// materialize reads every Data run of src and writes a sparse raw image. Zero
// and Hole runs are recorded but not written, so the file stays sparse.
func (c *MemoryCache) materialize(ctx context.Context, src fetch.Stream, size uint64, rawPath string) ([]memRun, error) {
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(c.dir, filepath.Base(rawPath)+".tmp-*")
	if err != nil {
		return nil, err
	}
	tmpName := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Truncate(int64(size)); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	buf := make([]byte, 1<<20)
	runs := make([]memRun, 0, 256)
	for off := uint64(0); off < size; {
		run, runErr := src.RunAt(off, size-off)
		if runErr != nil {
			_ = tmp.Close()
			return nil, runErr
		}
		end := run.End()
		if end <= off {
			_ = tmp.Close()
			return nil, fmt.Errorf("memory layer run did not advance at %d", off)
		}
		runs = append(runs, memRun{Off: off, End: end, Kind: int(run.Kind())})
		if run.Kind() == sparse.Data {
			length := end - run.Offset()
			for inner := uint64(0); inner < length; {
				n := len(buf)
				if remaining := int(length - inner); remaining < n {
					n = remaining
				}
				if err := ctx.Err(); err != nil {
					_ = tmp.Close()
					return nil, err
				}
				if _, err := run.ReadAt(ctx, buf[:n], inner); err != nil {
					_ = tmp.Close()
					return nil, err
				}
				if _, err := tmp.WriteAt(buf[:n], int64(run.Offset()+inner)); err != nil {
					_ = tmp.Close()
					return nil, err
				}
				inner += uint64(n)
			}
		}
		off = end
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpName, rawPath); err != nil {
		return nil, err
	}
	keep = true
	return runs, nil
}

// evict removes oldest entries (excluding keep) until the cache fits maxBytes.
// A single layer larger than the cap is kept; otherwise the cache could never
// hold a usable template. Unlinking a file another process has open is safe.
func (c *MemoryCache) evict(keep string) (entries int, freed int64) {
	if c.maxBytes <= 0 {
		return 0, 0
	}
	type entry struct {
		path string
		mod  time.Time
		size int64
	}
	var all []entry
	var total int64
	dirEntries, err := os.ReadDir(c.dir)
	if err != nil {
		return 0, 0
	}
	for _, de := range dirEntries {
		name := de.Name()
		if filepath.Ext(name) != ".raw" {
			continue
		}
		path := filepath.Join(c.dir, name)
		info, err := de.Info()
		if err != nil {
			continue
		}
		usage := info.Size()
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			// Sparse-aware: count allocated blocks, not the logical 2 GiB.
			usage = stat.Blocks * 512
		}
		if meta, err := os.Stat(strings.TrimSuffix(path, ".raw") + ".runs"); err == nil {
			usage += meta.Size()
		}
		all = append(all, entry{path: path, mod: info.ModTime(), size: usage})
		total += usage
	}
	if total <= c.maxBytes {
		return 0, 0
	}
	sort.Slice(all, func(i, j int) bool { return all[i].mod.Before(all[j].mod) })
	for _, e := range all {
		if e.path == keep {
			continue
		}
		if err := os.Remove(e.path); err != nil {
			continue
		}
		_ = os.Remove(strings.TrimSuffix(e.path, ".raw") + ".runs")
		entries++
		freed += e.size
		total -= e.size
		if total <= c.maxBytes {
			break
		}
	}
	return entries, freed
}

// rawFileStream reads a materialized layer. RunAt consults the sidecar's run
// map so Hole/Zero semantics are identical to the original layered source.
type rawFileStream struct {
	f    *os.File
	size uint64
	runs []memRun
}

func (s *rawFileStream) Size() uint64 { return s.size }
func (s *rawFileStream) Close() error { return s.f.Close() }

func (s *rawFileStream) RunAt(offset, limit uint64) (sparse.Run, error) {
	if offset >= s.size {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, errors.New("raw memory layer: zero limit")
	}
	i := sort.Search(len(s.runs), func(i int) bool { return s.runs[i].End > offset })
	if i == len(s.runs) {
		return nil, fmt.Errorf("raw memory layer: no run at %d", offset)
	}
	run := s.runs[i]
	if run.Off > offset {
		return nil, fmt.Errorf("raw memory layer: run map gap at %d", offset)
	}
	end := run.End
	if end > offset+limit {
		end = offset + limit
	}
	return &rawFileRun{stream: s, kind: sparse.RunKind(run.Kind), off: offset, end: end}, nil
}

func (s *rawFileStream) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= s.size {
		return 0, io.EOF
	}
	n := len(buf)
	if uint64(n) > s.size-offset {
		n = int(s.size - offset)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m, err := s.f.ReadAt(buf[:n], int64(offset))
	if err == io.EOF && m == n {
		err = nil
	}
	return m, err
}

type rawFileRun struct {
	stream *rawFileStream
	kind   sparse.RunKind
	off    uint64
	end    uint64
}

func (r *rawFileRun) Offset() uint64       { return r.off }
func (r *rawFileRun) End() uint64          { return r.end }
func (r *rawFileRun) Kind() sparse.RunKind { return r.kind }
func (r *rawFileRun) ReadAt(ctx context.Context, buf []byte, inner uint64) (int, error) {
	if r.kind != sparse.Data {
		clear(buf)
		return len(buf), nil
	}
	return r.stream.ReadAt(ctx, buf, r.off+inner)
}
