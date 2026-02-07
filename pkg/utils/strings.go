package utils

import (
	"hash/fnv"
	"strconv"
)

func FNVHashString(s string) string {
	h := fnv.New64a()
	h.Write([]byte(s))
	return strconv.FormatUint(h.Sum64(), 16)
}
