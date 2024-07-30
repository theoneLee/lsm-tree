package bloom_filter

import (
	"crypto/md5"
	"crypto/sha1"
	"math/big"
)

const (
	M           = 10000
	ByteVecSize = M / 8
	K           = 7
)

type BloomFilter struct {
	byteArr []byte
}

func New() *BloomFilter {
	return &BloomFilter{byteArr: make([]byte, ByteVecSize+1)}
}

func (f *BloomFilter) hash(key []byte, k int64) int64 {
	hash1 := md5.New()
	hash1.Write(key)
	res1 := hash1.Sum(nil)

	hash2 := sha1.New()
	hash2.Write(key)
	res2 := hash2.Sum(nil)

	h1 := bytesModInt(res1, M)
	h2 := bytesModInt(res2, M)

	return (h1 + k*h2) % M
}

func bytesModInt(bytes []byte, i int64) int64 {
	m := big.NewInt(i)

	num := new(big.Int).SetBytes(bytes)

	// 计算 num % m
	result := new(big.Int).Rem(num, m).Int64()

	return result
}

func (f *BloomFilter) Insert(key []byte) {
	// hash k次，得到在bitmap上的k个位置
	bitIndexes := make(map[int64]struct{}, K)
	for i := 0; i < K; i++ {
		h := f.hash(key, int64(i))
		bitIndexes[h] = struct{}{}
	}

	// 将bitmap的k个位置设置为1
	for bitIndex, _ := range bitIndexes {
		byteIndex := bitIndex / 8
		offset := bitIndex % 8

		f.byteArr[byteIndex] |= 1 << offset
	}

}

func (f *BloomFilter) MayContain(key []byte) bool {
	bitIndexes := make(map[int64]struct{}, K)
	for i := 0; i < K; i++ {
		h := f.hash(key, int64(i))
		bitIndexes[h] = struct{}{}
	}

	for bitIndex, _ := range bitIndexes {
		byteIndex := bitIndex / 8
		offset := bitIndex % 8

		if (f.byteArr[byteIndex]>>offset)&1 == 0 {
			return false
		}
	}
	return true
}
