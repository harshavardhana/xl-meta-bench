package main

import (
	"math/big"
	"strings"
	"time"
)

//go:generate msgp -file=$GOFILE -tests=false

type Format int

const (
	XL Format = iota
)

type JournalType int

const (
	Object JournalType = 0
	Delete JournalType = 1
	Link   JournalType = 2
)

type ErasureAlgo int

const (
	ReedSolomon ErasureAlgo = iota
)

type ChecksumAlgo int

const (
	HighwayHash256S ChecksumAlgo = iota
)

type ObjectMetaV2DeleteMarker struct {
	VersionID uint64 `json:"id"`
	ModTime   int64  `json:"mtime"`
}

type ObjectMetaV2Object struct {
	VersionID               uint64            `json:"id"`
	DataDir                 uint64            `json:"dataDir"`
	DataErasureAlgorithm    ErasureAlgo       `json:"ealgo"`
	DataErasureM            int               `json:"m"`
	DataErasureN            int               `json:"n"`
	DataErasureBlockSize    int               `json:"bsize"`
	DataErasureIndex        int               `json:"index"`
	DataErasureDistribution []int             `json:"dist"`
	DataErasureChecksumAlgo ChecksumAlgo      `json:"calgo"`
	DataPartInfoNumbers     []int             `json:"pnumbers"`
	DataPartInfoSizes       []int             `json:"psizes"`
	StatSize                int               `json:"size"`
	StatModTime             int64             `json:"mtime"`
	MetaSys                 map[string]string `json:"msys"`
	MetaUser                map[string]string `json:"muser"`
}

type ObjectMetaV2Link ObjectMetaV2Object

type ObjectMetaV2JournalEntry struct {
	Type         JournalType               `json:"type"`
	DeleteMarker *ObjectMetaV2DeleteMarker `json:"delete,omitempty"`
	Object       *ObjectMetaV2Object       `json:"object,omitempty"`
	Link         *ObjectMetaV2Link         `json:"link,omitempty"`
}

type ObjectMetaV2 struct {
	Version        int64                      `json:"version"` // Version of the current `object.json`.
	Format         Format                     `json:"format"`  // Format of the current `object.json`.
	ObjectJournals []ObjectMetaV2JournalEntry `json:"journals"`
}

func newObjectMetaV2Object(nparts int) *ObjectMetaV2Object {
	obj := &ObjectMetaV2Object{}
	var vid big.Int
	vid.SetString(strings.Replace("00000000-0000-0000-0000-000000000000", "-", "", 4), 16)
	obj.VersionID = vid.Uint64()
	var ddir big.Int
	ddir.SetString(strings.Replace("9dd7d884-121a-41e9-9a4e-d64e608d1b51", "-", "", 4), 16)
	obj.DataDir = ddir.Uint64()
	obj.DataErasureAlgorithm = ReedSolomon
	obj.DataErasureM = 8
	obj.DataErasureN = 8
	obj.DataErasureBlockSize = 10485760
	obj.DataErasureIndex = 1
	obj.DataErasureChecksumAlgo = HighwayHash256S
	obj.DataErasureDistribution = []int{
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
	obj.DataPartInfoSizes = make([]int, nparts)
	obj.DataPartInfoNumbers = make([]int, nparts)
	for j := 0; j < nparts; j++ {
		obj.DataPartInfoNumbers[j] = j + 1
		obj.DataPartInfoSizes[j] = 5242880
	}
	obj.StatSize = 52428800000
	obj.StatModTime = time.Now().Unix()
	obj.MetaSys = map[string]string{
		"minio-release": "DEVELOPMENT.GOGET",
	}
	obj.MetaUser = map[string]string{
		"content-type": "application/octet-stream",
		"etag":         "dc7cbd0700092050951b9063b94eb68a",
	}
	return obj
}

func newObjectMetaV2JournalEntry(nparts int) ObjectMetaV2JournalEntry {
	journal := ObjectMetaV2JournalEntry{
		Type:   Object,
		Object: newObjectMetaV2Object(nparts),
	}
	return journal
}

func newObjectMetaV2(nparts int, nversions int) ObjectMetaV2 {
	ObjectMeta := ObjectMetaV2{}
	ObjectMeta.Format = XL
	ObjectMeta.Version = 200
	ObjectMeta.ObjectJournals = make([]ObjectMetaV2JournalEntry, nversions)
	for i := 0; i < nversions; i++ {
		ObjectMeta.ObjectJournals[i] = newObjectMetaV2JournalEntry(nparts)
	}
	return ObjectMeta
}

func getSampleObjectMetaV2(nparts int, nversions int) ObjectMetaV2 {
	return newObjectMetaV2(nparts, nversions)
}
