package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/vmihailenco/msgpack/v4"
	"go.mongodb.org/mongo-driver/bson"
)

type xlMetaV2DeleteMarker struct {
	VersionID string    `json:"id"`
	ModTime   time.Time `json:"modTime"`
}

type xlMetaV2Object struct {
	VersionID string `json:"id"`
	Data      struct {
		Dir     string `json:"dir"`
		Erasure struct {
			Algorithm    string `json:"algorithm"`
			Data         int    `json:"data"`
			Parity       int    `json:"parity"`
			BlockSize    int    `json:"blockSize"`
			Index        int    `json:"index"`
			Distribution []int  `json:"distribution"`
			Checksum     struct {
				Algorithm string `json:"algorithm"`
			} `json:"checksum"`
		} `json:"erasure"`
		Parts []struct {
			Number int `json:"number"`
			Size   int `json:"size"`
		} `json:"parts"`
	} `json:"data"`
	Stat struct {
		Size    int       `json:"size"`
		ModTime time.Time `json:"modTime"`
	} `json:"stat"`
	Meta map[string]string `json:"meta"`
}

type xlMetaV2Link xlMetaV2Object

type xlMetaV2JournalEntry struct {
	Type         string               `json:"type"`
	DeleteMarker xlMetaV2DeleteMarker `json:"delete,omitempty"`
	Object       xlMetaV2Object       `json:"object,omitempty"`
	Link         xlMetaV2Link         `json:"link,omitempty"`
}

type xlMetaV2 struct {
	Version string `json:"version"` // Version of the current `xl.json`.
	Format  string `json:"format"`  // Format of the current `xl.json`.
	XL      struct {
		Journal []xlMetaV2JournalEntry `json:"journal"`
	} `json:"xl"`
}

func newXLMetaV2Object(nparts int) xlMetaV2Object {
	obj := xlMetaV2Object{}
	obj.VersionID = "00000000-0000-0000-0000-000000000000"
	obj.Data.Dir = "9dd7d884-121a-41e9-9a4e-d64e608d1b51"
	obj.Data.Erasure.Algorithm = "klauspost/reedsolomon/vandermonde"
	obj.Data.Erasure.Data = 8
	obj.Data.Erasure.Parity = 8
	obj.Data.Erasure.BlockSize = 10485760
	obj.Data.Erasure.Index = 1
	obj.Data.Erasure.Checksum.Algorithm = "highwayhash256S"
	obj.Data.Erasure.Distribution = []int{
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
		9,
		10,
		11,
		12,
		13,
		14,
		15,
		16,
	}
	type part struct {
		Number int `json:"number"`
		Size   int `json:"size"`
	}
	for j := 0; j < nparts; j++ {
		obj.Data.Parts = append(obj.Data.Parts, part{
			Number: j + 1,
			Size:   5242880,
		})
	}
	obj.Stat.Size = 52428800000
	obj.Stat.ModTime = time.Now()
	obj.Meta = map[string]string{
		"minio-release": "DEVELOPMENT.GOGET",
		"etag":          "dc7cbd0700092050951b9063b94eb68a",
		"content-type":  "application/octet-stream",
	}
	return obj
}

func newXLMetaV2JournalEntry(nparts int) xlMetaV2JournalEntry {
	journal := xlMetaV2JournalEntry{
		Type:   "object",
		Object: newXLMetaV2Object(nparts),
	}
	return journal
}

func newXLMetaV2(nparts int, nversions int) xlMetaV2 {
	xlMeta := xlMetaV2{}
	xlMeta.Format = "xl"
	xlMeta.Version = "2.0.0"
	for i := 0; i < nversions; i++ {
		xlMeta.XL.Journal = append(xlMeta.XL.Journal, newXLMetaV2JournalEntry(nparts))
	}
	return xlMeta
}

func getSampleXLMetaV2(nparts int, nversions int) xlMetaV2 {
	return newXLMetaV2(nparts, nversions)
}

func benchmarkParseUnmarshalN(b *testing.B, xlMetaBuf []byte, parser string) {
	b.SetBytes(int64(len(xlMetaBuf)))
	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(runtime.NumCPU())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var unMarshalXLMeta xlMetaV2
			switch parser {
			case "jsoniter":
				var json = jsoniter.ConfigCompatibleWithStandardLibrary
				if err := json.Unmarshal(xlMetaBuf, &unMarshalXLMeta); err != nil {
					b.Fatal(err)
				}
			case "encoding/json":
				if err := json.Unmarshal(xlMetaBuf, &unMarshalXLMeta); err != nil {
					b.Fatal(err)
				}
			case "bson":
				if err := bson.Unmarshal(xlMetaBuf, &unMarshalXLMeta); err != nil {
					b.Fatal(err)
				}
			case "msgpack":
				if err := msgpack.Unmarshal(xlMetaBuf, &unMarshalXLMeta); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func BenchmarkParseUnmarshalJsoniter(b *testing.B) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	for _, m := range []int{
		10,
		50,
		100,
		1000,
		10000,
	} {
		for _, n := range []int{
			10,
			50,
			100,
			1000,
		} {
			xlMetaBuf, err := json.Marshal(getSampleXLMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}
			test := fmt.Sprintf("%s-%dx%d", "jsoniter", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, xlMetaBuf, "jsoniter")
			})
		}
	}
}

func BenchmarkParseUnmarshalBson(b *testing.B) {
	for _, m := range []int{
		10,
		50,
		100,
		1000,
		10000,
	} {
		for _, n := range []int{
			10,
			50,
			100,
			1000,
		} {
			xlMetaBuf, err := bson.Marshal(getSampleXLMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}

			test := fmt.Sprintf("%s-%dx%d", "bson", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, xlMetaBuf, "bson")
			})
		}
	}
}

func BenchmarkParseUnmarshalMsgpack(b *testing.B) {
	for _, m := range []int{
		10,
		50,
		100,
		1000,
		10000,
	} {
		for _, n := range []int{
			10,
			50,
			100,
			1000,
		} {
			xlMetaBuf, err := msgpack.Marshal(getSampleXLMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}

			test := fmt.Sprintf("%s-%dx%d", "msgpack", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, xlMetaBuf, "msgpack")
			})
		}
	}
}
