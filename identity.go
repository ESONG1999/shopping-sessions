package main

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

const (
	packedBytes    = 9
	externalID     = uint64(1) << 63
	identityShards = 256
)

var identitySeed = maphash.MakeSeed()

// Short ASCII IDs are encoded losslessly and retain their bytewise sort order.
// All other IDs are interned by exact string equality.
type identities struct {
	shards [identityShards]identityShard
}

type identityShard struct {
	mu       sync.RWMutex
	extended map[string]uint64
	names    []string
}

func (ids *identities) key(s string) uint64 {
	if key := packIdentity(s); key != 0 || s == "" {
		return key
	}
	return ids.intern(s)
}

func packIdentity(s string) uint64 {
	if len(s) == 8 || len(s) == 9 {
		const ones, high = 0x0101010101010101, 0x8080808080808080
		word := binary.BigEndian.Uint64([]byte(s))
		if (word|((word-ones)&^word))&high == 0 {
			// Remove the unused high bit from each of eight ASCII bytes.
			pairs := (word&0x7f007f007f007f00)>>1 | word&0x007f007f007f007f
			halves := (pairs&0x3fff00003fff0000)>>2 | pairs&0x00003fff00003fff
			key := ((halves&0x0fffffff00000000)>>4 | halves&0x000000000fffffff) << 7
			if len(s) == 8 {
				return key
			}
			if last := s[8]; last > 0 && last < 128 {
				return key | uint64(last)
			}
		}
	}
	if len(s) <= packedBytes {
		var key uint64
		for i := 0; i < len(s); i++ {
			c := s[i]
			if c == 0 || c >= 128 {
				return 0
			}
			key = key<<7 | uint64(c)
		}
		return key << (7 * (packedBytes - len(s)))
	}
	return 0
}

func (ids *identities) intern(s string) uint64 {
	index := maphash.String(identitySeed, s) & (identityShards - 1)
	shard := &ids.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if key, ok := shard.extended[s]; ok {
		return key
	}
	if shard.extended == nil {
		shard.extended = make(map[string]uint64)
	}
	s = strings.Clone(s)
	key := externalID | uint64(len(shard.names))*identityShards | index
	shard.extended[s] = key
	shard.names = append(shard.names, s)
	return key
}

func (ids *identities) name(key uint64) string {
	if key&externalID != 0 {
		shard := &ids.shards[key&(identityShards-1)]
		shard.mu.RLock()
		name := shard.names[(key&^externalID)/identityShards]
		shard.mu.RUnlock()
		return name
	}
	var buf [packedBytes]byte
	n := 0
	for ; n < len(buf) && key != 0; n++ {
		buf[n] = byte(key>>56) & 127
		key = key << 7 &^ externalID
	}
	return string(buf[:n])
}

func (ids *identities) less(a, b uint64) bool {
	if (a|b)&externalID == 0 {
		return a < b
	}
	return ids.name(a) < ids.name(b)
}

type groupNode struct {
	parent uint32
	size   uint32
	name   uint64
}

type groups struct {
	index aliasTable
	nodes []groupNode
}

func (g *groups) add(key uint64) uint32 {
	hash := mix(key)
	pos := g.index.slot(hash, 0)
	if entry := g.index[pos]; entry.hash != 0 {
		return uint32(entry.person)
	}
	if len(g.nodes)*2 == len(g.index) {
		g.index = g.index.grow(0)
		pos = g.index.slot(hash, 0)
	}
	i := uint32(len(g.nodes))
	g.index[pos] = aliasEntry{hash, uint64(i)}
	g.nodes = append(g.nodes, groupNode{parent: i, size: 1, name: key})
	return i
}

func (g *groups) root(i uint32) uint32 {
	for g.nodes[i].parent != i {
		parent := g.nodes[i].parent
		g.nodes[i].parent = g.nodes[parent].parent
		i = parent
	}
	return i
}

func (g *groups) join(a, b uint64, ids *identities) {
	x, y := g.root(g.add(a)), g.root(g.add(b))
	if x == y {
		return
	}
	if g.nodes[x].size < g.nodes[y].size {
		x, y = y, x
	}
	g.nodes[y].parent = x
	g.nodes[x].size += g.nodes[y].size
	if ids.less(g.nodes[y].name, g.nodes[x].name) {
		g.nodes[x].name = g.nodes[y].name
	}
}

type aliasEntry struct {
	hash   uint64
	person uint64
}

type aliasTable []aliasEntry

func (a aliasTable) slot(hash uint64, shift uint) uint64 {
	mask := uint64(len(a) - 1)
	i := (hash >> shift) & mask
	for a[i].hash != 0 && a[i].hash != hash {
		i = (i + 1) & mask
	}
	return i
}

func (a aliasTable) grow(shift uint) aliasTable {
	grown := make(aliasTable, len(a)*2)
	for _, entry := range a {
		if entry.hash != 0 {
			grown[grown.slot(entry.hash, shift)] = entry
		}
	}
	return grown
}

type aliases struct {
	entries aliasTable
	filter  []uint64
}

func aliasBits(hash uint64) uint64 {
	return uint64(1)<<((hash>>52)&63) | uint64(1)<<(hash>>58)
}

func readAliases(dir string, workers int, ids *identities) (aliases, error) {
	paths, err := csvPaths(dir)
	if err != nil {
		return aliases{}, err
	}
	var bytes int64
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return aliases{}, err
		}
		bytes += info.Size()
	}
	// Estimate capacity from file sizes. Longer IDs, extra links, and small
	// components only affect allocation, and the table grows if needed.
	estimate := int(min(max(bytes/16, 32), 1<<22))
	g := groups{
		index: make(aliasTable, 1<<bits.Len(uint(2*estimate-1))),
		nodes: make([]groupNode, 0, estimate),
	}
	if err := scanPaths(paths, 1, 2, func(_ int, f []string) {
		if f[0] != "" && f[1] != "" {
			g.join(ids.key(f[0]), ids.key(f[1]), ids)
		}
	}); err != nil {
		return aliases{}, err
	}
	if len(g.nodes) == 0 {
		return aliases{}, nil
	}
	// Finalize names in place. The graph is immutable during this pass, so
	// each block can follow parent links without writing shared nodes.
	words := 1 << max(0, bits.Len(uint(len(g.nodes)-1))-2)
	filter := make([]uint64, words)
	localFilters := make([][]uint64, workers)
	localFilters[0] = filter
	const block = 16 << 10
	parallel(workers, (len(g.index)+block-1)/block, func(worker, job int) {
		local := localFilters[worker]
		if local == nil {
			local = make([]uint64, words)
			localFilters[worker] = local
		}
		for i := job * block; i < min((job+1)*block, len(g.index)); i++ {
			entry := &g.index[i]
			if entry.hash != 0 {
				n := uint32(entry.person)
				for g.nodes[n].parent != n {
					n = g.nodes[n].parent
				}
				entry.person = mix(g.nodes[n].name)
				// Only aliases with a different canonical ID need a table
				// probe. Bloom collisions fall through to exact lookup.
				if entry.hash != entry.person {
					local[(entry.hash>>bucketBits)&uint64(words-1)] |= aliasBits(entry.hash)
				}
			}
		}
	})
	for _, local := range localFilters[1:] {
		for i, bits := range local {
			filter[i] |= bits
		}
	}
	return aliases{entries: g.index, filter: filter}, nil
}

func (a aliases) personHash(key uint64) uint64 {
	return a.canonicalHash(mix(key))
}

func (a aliases) canonicalHash(hash uint64) uint64 {
	if len(a.filter) != 0 {
		mask := aliasBits(hash)
		if a.filter[(hash>>bucketBits)&uint64(len(a.filter)-1)]&mask == mask {
			entry := a.entries[a.entries.slot(hash, 0)]
			if entry.hash != 0 {
				return entry.person
			}
		}
	}
	return hash
}

type campaign struct {
	id, channel string
}

type catalog struct {
	products    map[string]uint32
	categories  []string
	unknown     uint32
	campaignIDs map[string]uint32
	campaigns   []campaign
}

func readCatalog(dataDir string) (catalog, error) {
	set := map[string]struct{}{"unknown": {}}
	products := make(map[string]string)
	if err := readPairs(filepath.Join(dataDir, "products.csv"), func(id, cat string) {
		products[id] = cat
		set[cat] = struct{}{}
	}); err != nil {
		return catalog{}, err
	}
	c := catalog{
		products:    make(map[string]uint32, len(products)),
		campaignIDs: make(map[string]uint32),
		campaigns:   []campaign{{"organic", "organic"}},
	}
	for name := range set {
		c.categories = append(c.categories, name)
	}
	slices.Sort(c.categories)
	categoryIDs := make(map[string]uint32, len(c.categories))
	for i, name := range c.categories {
		categoryIDs[name] = uint32(i)
	}
	c.unknown = categoryIDs["unknown"]
	for id, cat := range products {
		c.products[id] = categoryIDs[cat]
	}
	if err := readPairs(filepath.Join(dataDir, "campaigns.csv"), func(id, channel string) {
		c.campaignIDs[id] = uint32(len(c.campaigns))
		c.campaigns = append(c.campaigns, campaign{id, channel})
	}); err != nil {
		return catalog{}, err
	}
	return c, nil
}

func readPairs(path string, visit func(string, string)) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, text, _ := strings.Cut(string(data), "\n")
	malformed := false
	lines(text, func(line string) {
		var pair [2]string
		if !fields(line, pair[:]) {
			malformed = true
			return
		}
		visit(pair[0], pair[1])
	})
	if malformed {
		return fmt.Errorf("malformed catalog row in %s", path)
	}
	return nil
}
