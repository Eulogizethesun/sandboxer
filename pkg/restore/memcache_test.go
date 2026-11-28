package restore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type fakeRun struct {
	off  uint64
	end  uint64
	kind sparse.RunKind
	data []byte
}

type fakeLayer struct {
	size  uint64
	runs  []fakeRun
	reads int
}

func (f *fakeLayer) Size() uint64 { return f.size }
func (f *fakeLayer) Close() error { return nil }

func (f *fakeLayer) RunAt(offset, limit uint64) (sparse.Run, error) {
	if offset >= f.size {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, fmt.Errorf("zero limit")
	}
	for i := range f.runs {
		run := f.runs[i]
		if offset < run.off || offset >= run.end {
			continue
		}
		end := run.end
		if end > offset+limit {
			end = offset + limit
		}
		return &fakeRunHandle{layer: f, index: i, off: offset, end: end}, nil
	}
	return nil, fmt.Errorf("no run at %d", offset)
}

func (f *fakeLayer) ReadAt(ctx context.Context, buf []byte, offset uint64) (int, error) {
	run, err := f.RunAt(offset, uint64(len(buf)))
	if err != nil {
		return 0, err
	}
	n, err := run.ReadAt(ctx, buf, 0)
	return n, err
}

type fakeRunHandle struct {
	layer *fakeLayer
	index int
	off   uint64
	end   uint64
}

func (h *fakeRunHandle) Offset() uint64       { return h.off }
func (h *fakeRunHandle) End() uint64          { return h.end }
func (h *fakeRunHandle) Kind() sparse.RunKind { return h.layer.runs[h.index].kind }
func (h *fakeRunHandle) ReadAt(ctx context.Context, buf []byte, inner uint64) (int, error) {
	h.layer.reads++
	run := h.layer.runs[h.index]
	if run.kind != sparse.Data {
		clear(buf)
		return len(buf), nil
	}
	start := int(h.off-run.off) + int(inner)
	end := start + len(buf)
	if end > len(run.data) {
		end = len(run.data)
	}
	if start > len(run.data) {
		start = len(run.data)
	}
	return copy(buf, run.data[start:end]), nil
}

func testLayer(size uint64, dataRuns map[uint64][]byte) *fakeLayer {
	layer := &fakeLayer{size: size}
	// Sorted, contiguous runs: Data 0..4K, Zero 4K..8K, Data 8K..12K, Hole 12K..end.
	layer.runs = []fakeRun{
		{off: 0, end: 4096, kind: sparse.Data, data: dataRuns[0]},
		{off: 4096, end: 8192, kind: sparse.Zero},
		{off: 8192, end: 12288, kind: sparse.Data, data: dataRuns[8192]},
		{off: 12288, end: size, kind: sparse.Hole},
	}
	return layer
}

func TestMemoryCacheMaterializesAndServesSparseRuns(t *testing.T) {
	dir := t.TempDir()
	layer := testLayer(16384, map[uint64][]byte{
		0:    bytes.Repeat([]byte{'A'}, 4096),
		8192: bytes.Repeat([]byte{'B'}, 4096),
	})
	cache := newMemoryCache(dir, 0, t.Logf)
	ctx := context.Background()

	stream, cached := cache.wrap(ctx, nil, "manifest://layer-a", layer)
	if !cached {
		t.Fatal("first wrap did not materialize")
	}
	defer stream.Close()

	read := func(off uint64) []byte {
		t.Helper()
		buf := make([]byte, 4096)
		if _, err := stream.ReadAt(ctx, buf, off); err != nil {
			t.Fatalf("read %d: %v", off, err)
		}
		return buf
	}
	if got := read(0); got[0] != 'A' || got[4095] != 'A' {
		t.Fatalf("data run 0 = %q", got[:4])
	}
	if got := read(4096); !bytes.Equal(got, make([]byte, 4096)) {
		t.Fatal("zero run did not read as zero")
	}
	if got := read(8192); got[0] != 'B' || got[4095] != 'B' {
		t.Fatalf("data run 8192 = %q", got[:4])
	}
	if got := read(12288); !bytes.Equal(got, make([]byte, 4096)) {
		t.Fatal("hole run did not read as zero")
	}

	for _, tc := range []struct {
		off  uint64
		kind sparse.RunKind
	}{
		{0, sparse.Data},
		{4096, sparse.Zero},
		{8192, sparse.Data},
		{12288, sparse.Hole},
	} {
		run, err := stream.RunAt(tc.off, 4096)
		if err != nil {
			t.Fatalf("RunAt(%d): %v", tc.off, err)
		}
		if run.Kind() != tc.kind {
			t.Fatalf("RunAt(%d) kind = %v, want %v", tc.off, run.Kind(), tc.kind)
		}
	}

	key := memoryCacheKey("manifest://layer-a", 16384)
	info, err := os.Stat(filepath.Join(dir, key+".raw"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 16384 {
		t.Fatalf("raw logical size = %d, want 16384", info.Size())
	}
	if _, err := os.Stat(filepath.Join(dir, key+".runs")); err != nil {
		t.Fatalf("missing run map: %v", err)
	}

	// A second wrap must hit without touching the source.
	second := testLayer(16384, map[uint64][]byte{
		0:    bytes.Repeat([]byte{'A'}, 4096),
		8192: bytes.Repeat([]byte{'B'}, 4096),
	})
	stream2, cached := cache.wrap(ctx, nil, "manifest://layer-a", second)
	if !cached {
		t.Fatal("second wrap did not hit")
	}
	defer stream2.Close()
	if second.reads != 0 {
		t.Fatalf("cache hit read the source %d times", second.reads)
	}
}

func TestMemoryCacheSkipsFileLayers(t *testing.T) {
	dir := t.TempDir()
	layer := testLayer(16384, nil)
	cache := newMemoryCache(dir, 0, t.Logf)
	stream, cached := cache.wrap(context.Background(), nil, "file:///tmp/checkpoint.snapshot@digest:abc", layer)
	if cached || stream != layer {
		t.Fatal("file:// layer must not be cached")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cache dir has %d entries for a file:// layer", len(entries))
	}
}

func TestMemoryCacheCorruptRunMapRematerializes(t *testing.T) {
	dir := t.TempDir()
	layer := testLayer(16384, map[uint64][]byte{0: bytes.Repeat([]byte{'A'}, 4096)})
	cache := newMemoryCache(dir, 0, t.Logf)
	ctx := context.Background()
	stream, cached := cache.wrap(ctx, nil, "manifest://layer-b", layer)
	if !cached {
		t.Fatal("materialize failed")
	}
	_ = stream.Close()
	key := memoryCacheKey("manifest://layer-b", 16384)
	if err := os.Remove(filepath.Join(dir, key+".runs")); err != nil {
		t.Fatal(err)
	}

	second := testLayer(16384, map[uint64][]byte{0: bytes.Repeat([]byte{'A'}, 4096)})
	stream2, cached := cache.wrap(ctx, nil, "manifest://layer-b", second)
	if !cached {
		t.Fatal("rematerialize failed")
	}
	defer stream2.Close()
	if second.reads == 0 {
		t.Fatal("missing run map did not force a source read")
	}
}

func TestMemoryCacheEvictsOldestEntry(t *testing.T) {
	dir := t.TempDir()
	cache := newMemoryCache(dir, 1, t.Logf)
	ctx := context.Background()

	first := testLayer(16384, map[uint64][]byte{0: bytes.Repeat([]byte{'A'}, 4096)})
	stream, cached := cache.wrap(ctx, nil, "manifest://layer-old", first)
	if !cached {
		t.Fatal("first materialize failed")
	}
	_ = stream.Close()
	oldRaw := filepath.Join(dir, memoryCacheKey("manifest://layer-old", 16384)+".raw")
	oldTime := time.Now().Add(-time.Hour)
	if err := os.Chtimes(oldRaw, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	second := testLayer(16384, map[uint64][]byte{0: bytes.Repeat([]byte{'B'}, 4096)})
	stream2, cached := cache.wrap(ctx, nil, "manifest://layer-new", second)
	if !cached {
		t.Fatal("second materialize failed")
	}
	defer stream2.Close()

	if _, err := os.Stat(oldRaw); !os.IsNotExist(err) {
		t.Fatalf("old entry was not evicted: %v", err)
	}
	newRaw := filepath.Join(dir, memoryCacheKey("manifest://layer-new", 16384)+".raw")
	if _, err := os.Stat(newRaw); err != nil {
		t.Fatalf("new entry missing: %v", err)
	}
}
