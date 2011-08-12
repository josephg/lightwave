package lightwave

import (
  "encoding/binary"
  "hash"
)

type BloomFilter struct {
  filter []byte
  k      int
  alg    hash.Hash
}

func NewBloomFilter(alg hash.Hash) (b *BloomFilter) {
  b = &BloomFilter{alg: alg, k: 5}
  b.filter = make([]byte, 1000)
  return
}

func (self *BloomFilter) Load(bytes []byte) {
  if len(self.filter) != len(bytes) {
    panic("Wrong number of bytes")
  }
  self.filter = bytes
}

func (self *BloomFilter) Bytes() []byte {
  return self.filter
}

// Returns the size in bytes of the filter
func (self *BloomFilter) Size() int {
  return len(self.filter)
}

// Return the number of hashes used in this filter
func (self *BloomFilter) Hashes() int {
  return self.k
}

// Add the byte array to the filter
func (self *BloomFilter) Add(buf []byte) {
  salt := make([]byte, 4)
  for i := 0; i < self.k; i++ {
    self.alg.Reset()
    binary.BigEndian.PutUint32(salt, uint32(i+1))
    self.alg.Write(salt)
    self.alg.Write(buf)
    hash := self.alg.Sum()
    bit := binary.BigEndian.Uint32(hash[0:4]) % uint32(len(self.filter) * 8)
    self.filter[bit/8] |= 1 << (bit % 8)
  }
}

// Check if the byte array may be present in the filter
func (self *BloomFilter) Has(buf []byte) bool {
  salt := make([]byte, 4)
  for i := 0; i < self.k; i++ {
    self.alg.Reset()
    binary.BigEndian.PutUint32(salt, uint32(i+1))
    self.alg.Write(salt)
    self.alg.Write(buf)
    hash := self.alg.Sum()
    bit := binary.BigEndian.Uint32(hash[0:4]) % uint32(len(self.filter) * 8)
    if self.filter[bit/8] & (1 << (uint(bit) % 8)) == 0 {
      return false
    }
  }
  return true
}