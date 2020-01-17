package main

import "time"

//go:generate msgp -file=$GOFILE -tests=false

type ErasureAlgo int

const (
	ReedSolomon ErasureAlgo = iota
)

type ChecksumAlgo int

const (
	HighwayHash256S ChecksumAlgo = iota
)

type XLMetaV2DeleteMarker struct {
	VersionID string    `json:"id"`
	ModTime   time.Time `json:"modTime"`
}

type XLMetaV2Object struct {
	VersionID string `json:"id"`
	Data      struct {
		Dir     string `json:"dir"`
		Erasure struct {
			Algorithm    ErasureAlgo `json:"algorithm"`
			Data         int         `json:"data"`
			Parity       int         `json:"parity"`
			BlockSize    int         `json:"blockSize"`
			Index        int         `json:"index"`
			Distribution []int       `json:"distribution"`
			Checksum     struct {
				Algorithm ChecksumAlgo `json:"algorithm"`
			} `json:"checksum"`
		} `json:"erasure"`
		Parts []struct {
			Number int `json:"number"`
			Size   int `json:"size"`
		} `json:"parts"`
	} `json:"data"`
	Stat struct {
		Size    int   `json:"size"`
		ModTime int64 `json:"modTime"`
	} `json:"stat"`
	Meta struct {
		Sys  map[string]string `json:"sys"`
		User map[string]string `json:"user"`
	} `json:"meta"`
}

type XLMetaV2Link XLMetaV2Object

type XLMetaV2JournalEntry struct {
	Type         string               `json:"type"`
	DeleteMarker XLMetaV2DeleteMarker `json:"delete,omitempty"`
	Object       XLMetaV2Object       `json:"object,omitempty"`
	Link         XLMetaV2Link         `json:"link,omitempty"`
}

type XLMetaV2 struct {
	Version string `json:"version"` // Version of the current `xl.json`.
	Format  string `json:"format"`  // Format of the current `xl.json`.
	XL      struct {
		Journal []XLMetaV2JournalEntry `json:"journal"`
	} `json:"xl"`
}

func newXLMetaV2Object(nparts int) XLMetaV2Object {
	obj := XLMetaV2Object{}
	obj.VersionID = "00000000-0000-0000-0000-000000000000"
	obj.Data.Dir = "9dd7d884-121a-41e9-9a4e-d64e608d1b51"
	obj.Data.Erasure.Algorithm = ReedSolomon
	obj.Data.Erasure.Data = 8
	obj.Data.Erasure.Parity = 8
	obj.Data.Erasure.BlockSize = 10485760
	obj.Data.Erasure.Index = 1
	obj.Data.Erasure.Checksum.Algorithm = HighwayHash256S
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
	obj.Stat.ModTime = time.Now().Unix()
	obj.Meta.Sys = map[string]string{
		"minio-release": "DEVELOPMENT.GOGET",
		"etag":          "dc7cbd0700092050951b9063b94eb68a",
	}
	obj.Meta.User = map[string]string{
		"content-type": "application/octet-stream",
	}
	return obj
}

func newXLMetaV2JournalEntry(nparts int) XLMetaV2JournalEntry {
	journal := XLMetaV2JournalEntry{
		Type:   "object",
		Object: newXLMetaV2Object(nparts),
	}
	return journal
}

func newXLMetaV2(nparts int, nversions int) XLMetaV2 {
	XLMeta := XLMetaV2{}
	XLMeta.Format = "xl"
	XLMeta.Version = "2.0.0"
	for i := 0; i < nversions; i++ {
		XLMeta.XL.Journal = append(XLMeta.XL.Journal, newXLMetaV2JournalEntry(nparts))
	}
	return XLMeta
}

func getSampleXLMetaV2(nparts int, nversions int) XLMetaV2 {
	return newXLMetaV2(nparts, nversions)
}
