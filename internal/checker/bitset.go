package checker

// A bitset records which operations have been linearized so far. It is half of
// the search's cache key, and the search sets and clears bits on every step, so
// it is a fixed-width []uint64 rather than a map: the whole point of the cache
// is that testing membership must cost less than exploring the branch again.
type bitset []uint64

func newBitset(n int) bitset {
	return make(bitset, (n+63)/64)
}

func (b bitset) set(i int)   { b[i>>6] |= 1 << uint(i&63) }
func (b bitset) clear(i int) { b[i>>6] &^= 1 << uint(i&63) }

func (b bitset) clone() bitset {
	out := make(bitset, len(b))
	copy(out, b)
	return out
}

func (b bitset) equal(o bitset) bool {
	if len(b) != len(o) {
		return false
	}
	for i, w := range b {
		if w != o[i] {
			return false
		}
	}
	return true
}

// hash mixes the words with the FNV-1a prime, a word at a time rather than a
// byte at a time. This is only a bucket index - every hit is confirmed with a
// full comparison - so speed matters and distribution quality does not.
func (b bitset) hash() uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, w := range b {
		h ^= w
		h *= prime64
	}
	return h
}
