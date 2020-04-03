package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"runtime"
	"testing"

	"github.com/dustin/go-humanize"
	jsoniter "github.com/json-iterator/go"
	"gopkg.in/mgo.v2/bson"
)

func benchmarkParseUnmarshalN(b *testing.B, ObjectMetaBuf []byte, parser string, elems int) {
	b.SetBytes(int64(elems))
	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(runtime.NumCPU())
	if testing.Verbose() {
		b.Log(parser, "Size:", humanize.IBytes(uint64(len(ObjectMetaBuf))))
	}
	b.RunParallel(func(pb *testing.PB) {
		var journal *ObjectMetaV2JournalEntry
		for pb.Next() {
			var unMarshalObjectMeta ObjectMetaV2
			switch parser {
			case "bson":
				if err := bson.Unmarshal(ObjectMetaBuf, &unMarshalObjectMeta); err != nil {
					b.Fatal(err)
				}
				if unMarshalObjectMeta.ObjectJournals[0].Object.DataErasureM != 8 {
					b.Fatal("unexpected")
				}
			case "jsoniter-fast":
				var json = jsoniter.ConfigFastest
				if err := json.Unmarshal(ObjectMetaBuf, &unMarshalObjectMeta); err != nil {
					b.Fatal(err)
				}
				if unMarshalObjectMeta.ObjectJournals[0].Object.DataErasureM != 8 {
					b.Fatal("unexpected")
				}
			case "jsoniter-compat":
				var json = jsoniter.ConfigCompatibleWithStandardLibrary
				if err := json.Unmarshal(ObjectMetaBuf, &unMarshalObjectMeta); err != nil {
					b.Fatal(err)
				}
				if unMarshalObjectMeta.ObjectJournals[0].Object.DataErasureM != 8 {
					b.Fatal("unexpected")
				}
			case "gob":
				dec := gob.NewDecoder(bytes.NewReader(ObjectMetaBuf))
				if err := dec.Decode(&unMarshalObjectMeta); err != nil {
					b.Fatal(err)
				}
				if unMarshalObjectMeta.ObjectJournals[0].Object.DataErasureM != 8 {
					b.Fatal("unexpected")
				}
			case "msgpack-fast":
				_, err := unMarshalObjectMeta.UnmarshalMsg(ObjectMetaBuf)
				if err != nil {
					b.Fatal(err)
				}
				if unMarshalObjectMeta.ObjectJournals[0].Object.DataErasureM != 8 {
					b.Fatal("unexpected")
				}
				if len(unMarshalObjectMeta.ObjectJournals)*len(unMarshalObjectMeta.ObjectJournals[0].Object.DataPartInfoNumbers) != elems {
					b.Fatalf("unexpected, len (%d * %d) != want (%d)", len(unMarshalObjectMeta.ObjectJournals), len(unMarshalObjectMeta.ObjectJournals[0].Object.DataPartInfoNumbers), elems)
				}
			case "msgpack-last":
				var err error
				journal, err = unMarshalObjectMeta.GetJournalEntryN(ObjectMetaBuf, -1, journal)
				if err != nil {
					b.Fatal(err)
				}
				if journal.Object.DataErasureM != 8 {
					b.Fatal("unexpected")
				}
			}
		}
	})
}

var (
	ms = []int{
		1,
		50,
		1000,
		10000,
	}

	ns = []int{
		1,
		50,
		1000,
		10000,
	}
)

func BenchmarkParseUnmarshalGob(b *testing.B) {
	for _, m := range ms {
		for _, n := range ns {
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			if err := enc.Encode(getSampleObjectMetaV2(m, n)); err != nil {
				b.Fatal(err)
			}
			test := fmt.Sprintf("%s-%dx%d", "gob", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, buf.Bytes(), "gob", n*m)
			})
		}
	}
}

func BenchmarkParseUnmarshalBson(b *testing.B) {
	for _, m := range ms {
		for _, n := range ns {
			ObjectMetaBuf, err := bson.Marshal(getSampleObjectMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}
			test := fmt.Sprintf("%s-%dx%d", "bson", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, ObjectMetaBuf, "bson", n*m)
			})
		}
	}
}

func BenchmarkParseUnmarshalJsoniterFast(b *testing.B) {
	var json = jsoniter.ConfigFastest
	for _, m := range ms {
		for _, n := range ns {
			ObjectMetaBuf, err := json.Marshal(getSampleObjectMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}
			test := fmt.Sprintf("%s-%dx%d", "jsoniter-fast", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, ObjectMetaBuf, "jsoniter-fast", n*m)
			})
		}
	}
}

func BenchmarkParseUnmarshalJsoniterCompat(b *testing.B) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	for _, m := range ms {
		for _, n := range ns {
			ObjectMetaBuf, err := json.Marshal(getSampleObjectMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}
			test := fmt.Sprintf("%s-%dx%d", "jsoniter-compat", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, ObjectMetaBuf, "jsoniter-compat", n*m)
			})
		}
	}
}

func BenchmarkParseUnmarshalTinylibMsg(b *testing.B) {
	for _, m := range ms {
		for _, n := range ns {
			xlmeta := getSampleObjectMetaV2(m, n)
			ObjectMetaBuf, err := xlmeta.MarshalMsg(nil)
			if err != nil {
				b.Fatal(err)
			}

			test := fmt.Sprintf("%s-%dx%d", "msgpack-fast", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, ObjectMetaBuf, "msgpack-fast", n*m)
			})
		}
	}
}

func BenchmarkParseUnmarshalLastTinylibMsg(b *testing.B) {
	for _, m := range ms {
		for _, n := range ns {
			xlmeta := getSampleObjectMetaV2(m, n)
			ObjectMetaBuf, err := xlmeta.MarshalMsg(nil)
			if err != nil {
				b.Fatal(err)
			}

			test := fmt.Sprintf("%s-%dx%d", "msgpack-last", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, ObjectMetaBuf, "msgpack-last", n*m)
			})
		}
	}
}
