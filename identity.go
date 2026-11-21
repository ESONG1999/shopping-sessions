package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

const (
	packedBytes = 9
	externalID  = uint64(1) << 63
)

// Short ASCII IDs are encoded losslessly and retain their bytewise sort order.
// All other IDs are interned by exact string equality.
type identities struct {
	mu       sync.RWMutex
	extended map[string]uint64
	names    []string
}

func (ids *identities) key(s string) uint64 {
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
				return ids.intern(s)
			}
			key = key<<7 | uint64(c)
		}
		return key << (7 * (packedBytes - len(s)))
	}
	return ids.intern(s)
}

func (ids *identities) intern(s string) uint64 {
	ids.mu.Lock()
	defer ids.mu.Unlock()
	if key, ok := ids.extended[s]; ok {
		return key
	}
	if ids.extended == nil {
		ids.extended = make(map[string]uint64)
	}
	s = strings.Clone(s)
	key := externalID | uint64(len(ids.names))
	ids.extended[s] = key
	ids.names = append(ids.names, s)
	return key
}

func (ids *identities) name(key uint64) string {
	if key&externalID != 0 {
		ids.mu.RLock()
		name := ids.names[key&^externalID]
		ids.mu.RUnlock()
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
	index map[uint64]uint32
	nodes []groupNode
}

func (g *groups) add(key uint64) uint32 {
	if i, ok := g.index[key]; ok {
		return i
	}
	i := uint32(len(g.nodes))
	g.index[key] = i
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

type aliases map[uint64]uint64

func readAliases(dir string, ids *identities) (aliases, error) {
	g := groups{index: make(map[uint64]uint32)}
	if err := scanParts(dir, 1, 2, func(_ int, f []string) {
		if f[0] != "" && f[1] != "" {
			g.join(ids.key(f[0]), ids.key(f[1]), ids)
		}
	}); err != nil {
		return nil, err
	}
	result := make(aliases, len(g.index))
	for key, i := range g.index {
		if name := g.nodes[g.root(i)].name; name != key {
			result[key] = name
		}
	}
	return result, nil
}

func (a aliases) person(key uint64) uint64 {
	if name := a[key]; name != 0 {
		return name
	}
	return key
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
