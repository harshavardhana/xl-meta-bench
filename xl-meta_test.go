package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"runtime"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/vmihailenco/msgpack/v4"
)

func benchmarkParseUnmarshalN(b *testing.B, XLMetaBuf []byte, parser string) {
	b.SetBytes(int64(len(XLMetaBuf)))
	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(runtime.NumCPU())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var unMarshalXLMeta XLMetaV2
			switch parser {
			case "jsoniter-fast":
				var json = jsoniter.ConfigFastest
				if err := json.Unmarshal(XLMetaBuf, &unMarshalXLMeta); err != nil {
					b.Fatal(err)
				}
			case "jsoniter-compat":
				var json = jsoniter.ConfigCompatibleWithStandardLibrary
				if err := json.Unmarshal(XLMetaBuf, &unMarshalXLMeta); err != nil {
					b.Fatal(err)
				}
			case "gob":
				buf := bytes.NewBuffer(XLMetaBuf)
				dec := gob.NewDecoder(buf)
				if err := dec.Decode(&unMarshalXLMeta); err != nil {
					b.Fatal(err)
				}
			case "msgpack-fast":
				_, err := unMarshalXLMeta.UnmarshalMsg(XLMetaBuf)
				if err != nil {
					b.Fatal(err)
				}
			case "msgpack":
				if err := msgpack.Unmarshal(XLMetaBuf, &unMarshalXLMeta); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func BenchmarkParseUnmarshalGob(b *testing.B) {
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
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			if err := enc.Encode(getSampleXLMetaV2(m, n)); err != nil {
				b.Fatal(err)
			}
			test := fmt.Sprintf("%s-%dx%d", "gob", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, buf.Bytes(), "gob")
			})
		}
	}
}

func BenchmarkParseUnmarshalJsoniterFast(b *testing.B) {
	var json = jsoniter.ConfigFastest
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
			XLMetaBuf, err := json.Marshal(getSampleXLMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}
			test := fmt.Sprintf("%s-%dx%d", "jsoniter-fast", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, XLMetaBuf, "jsoniter-fast")
			})
		}
	}
}

func BenchmarkParseUnmarshalJsoniterCompat(b *testing.B) {
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
			XLMetaBuf, err := json.Marshal(getSampleXLMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}
			test := fmt.Sprintf("%s-%dx%d", "jsoniter-compat", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, XLMetaBuf, "jsoniter-compat")
			})
		}
	}
}

func BenchmarkParseUnmarshalTinylibMsg(b *testing.B) {
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
			xlmeta := getSampleXLMetaV2(m, n)
			XLMetaBuf, err := xlmeta.MarshalMsg(nil)
			if err != nil {
				b.Fatal(err)
			}

			test := fmt.Sprintf("%s-%dx%d", "msgpack-fast", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, XLMetaBuf, "msgpack-fast")
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
			XLMetaBuf, err := msgpack.Marshal(getSampleXLMetaV2(m, n))
			if err != nil {
				b.Fatal(err)
			}

			test := fmt.Sprintf("%s-%dx%d", "msgpack", m, n)
			b.Run(test, func(b *testing.B) {
				benchmarkParseUnmarshalN(b, XLMetaBuf, "msgpack")
			})
		}
	}
}
