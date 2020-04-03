package main

import (
	"errors"
	"math/big"
	"math/rand"
	"strings"
	"time"

	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp -file=$GOFILE -tests=false

type Format uint8

const (
	XL Format = iota
)

type JournalType uint8

const (
	Object JournalType = 0
	Delete JournalType = 1
	Link   JournalType = 2
)

type ErasureAlgo uint8

const (
	ReedSolomon ErasureAlgo = iota
)

type ChecksumAlgo uint8

const (
	HighwayHash256S ChecksumAlgo = iota
)

type ObjectMetaV2DeleteMarker struct {
	VersionID uint64 `json:"id" msg:"id"`
	ModTime   int64  `json:"mtime" msg:"mtime"`
}

// DeltaEncodedInt is an integer array that will be serialized as delta-encoded values.
//msgp:ignore DeltaEncodedInt
type DeltaEncodedInt []int

type ObjectMetaV2Object struct {
	VersionID               uint64              `json:"id" msg:"id"`
	DataDir                 uint64              `json:"dd" msg:"dd"`
	DataErasureAlgorithm    ErasureAlgo         `json:"ealgo" msg:"ealgo"`
	DataErasureM            int                 `json:"m" msg:"m"`
	DataErasureN            int                 `json:"n" msg:"n"`
	DataErasureBlockSize    int                 `json:"bsize" msg:"bsize"`
	DataErasureIndex        int                 `json:"index" msg:"index"`
	DataErasureDistribution []uint8             `json:"dist" msg:"dist"`
	DataErasureChecksumAlgo ChecksumAlgo        `json:"calgo" msg:"clago"`
	DataPartInfoNumbers     DeltaEncodedInt     `json:"pnum" msg:"pnum"`
	DataPartInfoSizes       DeltaEncodedInt     `json:"psz" msg:"psz"`
	StatSize                int                 `json:"size" msg:"size"`
	StatModTime             int64               `json:"mtime" msg:"mtime"`
	MetaSys                 map[string][]byte   `json:"msys" msg:"msys,omitempty"`
	MetaUser                map[string][]string `json:"muser" msg:"muser,omitempty"`
}

type ObjectMetaV2Link ObjectMetaV2Object

type ObjectMetaV2JournalEntry struct {
	Type         JournalType               `json:"type" msg:"type"`
	DeleteMarker *ObjectMetaV2DeleteMarker `json:"delete,omitempty" msg:"delete,omitempty"`
	Object       *ObjectMetaV2Object       `json:"object,omitempty" msg:"object,omitempty"`
	Link         *ObjectMetaV2Link         `json:"link,omitempty" msg:"link,omitempty"`
}

type ObjectMetaV2 struct {
	Version        int64                      `json:"v" msg:"v"`     // Version of the current `object.json`.
	Format         Format                     `json:"fmt" msg:"fmt"` // Format of the current `object.json`.
	ObjectJournals []ObjectMetaV2JournalEntry `json:"ojs" msg:"ojs"`
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
	obj.DataErasureDistribution = []uint8{
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
	obj.MetaUser = map[string][]string{
		"content-type": []string{"application/octet-stream"},
		"etag":         []string{"dc7cbd0700092050951b9063b94eb68a"},
	}
	buf := make([]byte, 32)
	rand.Read(buf)
	obj.MetaSys = map[string][]byte{
		"minio-release": []byte("DEVELOPMENT.GOGET"),
		"mac":           []byte("hmac-sha256: xxxxxxxxxxxxxxxxxxxxxxx"),
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

// GetJournalEntryN returns journal entry n.
// z will be filled with the global information, but z.Journals will not be filled.
// Specify version -1 to get the last version.
// An optional destination can be supplied.
func (z *ObjectMetaV2) GetJournalEntryN(bts []byte, n int, dst *ObjectMetaV2JournalEntry) (journal *ObjectMetaV2JournalEntry, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "v":
			z.Version, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Version")
				return
			}
		case "fmt":
			{
				var zb0002 int
				zb0002, bts, err = msgp.ReadIntBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Format")
					return
				}
				z.Format = Format(zb0002)
			}
		case "journals":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ObjectJournals")
				return
			}
			if n < 0 {
				// last entry
				n = int(zb0003) - 1
			}
			if n > int(zb0003)-1 {
				err = msgp.WrapError(errors.New("requested object index not found"), "ObjectJournals", zb0003)
				return
			}
			if dst == nil {
				dst = &ObjectMetaV2JournalEntry{}
			}
			for n >= 0 {
				// Actually decoding is faster than skipping....
				bts, err = dst.UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "ObjectJournals")
					return
				}
				if n == 0 {
					journal = dst
					return
				}
				n--
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *DeltaEncodedInt) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(DeltaEncodedInt, zb0002)
	}
	var c int
	for zb0001 := range *z {
		var v int
		v, err = dc.ReadInt()
		c += v
		(*z)[zb0001] = c
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z DeltaEncodedInt) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	var c int
	for zb0003 := range z {
		v := z[zb0003]
		err = en.WriteInt(v - c)
		if err != nil {
			err = msgp.WrapError(err, zb0003)
			return
		}
		c = v
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z DeltaEncodedInt) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	var c int
	for zb0003 := range z {
		v := z[zb0003]
		o = msgp.AppendInt(o, v-c)
		c = v
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *DeltaEncodedInt) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(DeltaEncodedInt, zb0002)
	}
	var c int
	for zb0001 := range *z {
		var v int
		v, bts, err = msgp.ReadIntBytes(bts)
		(*z)[zb0001] = c + v
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z DeltaEncodedInt) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize + (len(z) * (msgp.IntSize))
	return
}
