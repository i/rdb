package rdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
)

const MetaSize = 150

type DB interface {
	Set(string, interface{}) error
	Get(string) (interface{}, error)
	Key(bool) []MetaInfo
}

type db struct {
	metaKeys map[string]MetaInfo
	files    map[string]*os.File
	cf       *os.File
	dirpath  string
	m        sync.Mutex
}

func NewDB(dirpath string) (DB, error) {
	db := &db{
		metaKeys: make(map[string]MetaInfo),
		files:    make(map[string]*os.File),
		dirpath:  dirpath,
	}
	if err := db.replay(); err != nil {
		return nil, err
	}
	if err := db.setCf(); err != nil {
		return nil, err
	}
	return db, nil
}

type MetaInfo struct {
	Key    string
	Ts     time.Time
	Path   string
	Offset int64
	VSize  int
}

func (d *db) Set(key string, v interface{}) error {
	if d.cf == nil {
		if err := d.setCf(); err != nil {
			return err
		}
	}

	offset, err := d.cf.Seek(0, 2)
	if err != nil {
		return err
	}

	vbuf, err := json.Marshal(v)
	if err != nil {
		return err
	}

	meta := MetaInfo{
		Key:    key,
		Ts:     time.Now(),
		Path:   path.Join(d.cf.Name()),
		Offset: offset,
		VSize:  len(vbuf),
	}

	mbuf, err := newMetaBuffer(meta)
	if err != nil {
		return err
	}

	buf := append(mbuf, vbuf...)
	_, err = d.cf.Write(buf)
	if err != nil {
		return err
	}

	d.metaKeys[key] = meta
	return nil
}

func (d *db) Get(key string) (interface{}, error) {
	meta, ok := d.metaKeys[key]
	if !ok {
		return nil, ErrNotFound
	}

	file, ok := d.files[meta.Path]
	if !ok {
		f, err := os.Open(meta.Path)
		if err != nil {
			return nil, err
		}
		file = f
	}
	return readValue(meta, file)
}

func (d *db) Keys() []string {
	var keys []string
	for k, _ := range d.metaKeys {
		keys = append(keys, k)
	}
	return keys
}

func (d *db) replay() error {
	f, err := os.Open(d.dirpath)
	if err != nil {
		return err
	}

	names, err := f.Readdirnames(0)
	if err != nil {
		return err
	}

	sort.Strings(names)
	for _, name := range names {
		if err := d.loadFile(path.Join(d.dirpath, name)); err != nil {
			return err
		}
	}
	return nil
}

func (d *db) loadFile(name string) error {
	var offset int64
	var meta MetaInfo

	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		meta, offset, err = metaFromOffset(f, offset)
		fmt.Println(meta, offset, err)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		d.metaKeys[meta.Key] = meta
	}

	return nil
}

func (d *db) setCf() error {
	p := path.Join(d.dirpath, fmt.Sprintf("%d", time.Now().UTC().Unix()))
	cf, err := os.Create(p)
	if err != nil {
		return err
	}
	d.files[p] = cf
	d.cf = cf
	return nil
}

func newMetaBuffer(meta MetaInfo) ([]byte, error) {
	buf := make([]byte, 5)
	mbuf, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	n := binary.PutVarint(buf, int64(len(mbuf)))
	b := bytes.NewBuffer(buf)
	b.Truncate(n)
	b.Write(mbuf)
	return b.Bytes(), nil
}

func metaFromOffset(file *os.File, offset int64) (MetaInfo, int64, error) {
	var info MetaInfo
	buf := make([]byte, 5) // todo
	if _, err := file.ReadAt(buf, offset); err != nil {
		return info, 0, err
	}

	// Calculate offset for actual value
	msize, err := binary.ReadVarint(bytes.NewBuffer(buf))
	if err != nil {
		return info, 0, err
	}
	vOffset := msize + 2
	buf = make([]byte, msize)
	if _, err := file.ReadAt(buf, offset+2); err != nil {
		return info, 0, err
	}
	json.Unmarshal(buf, &info)
	return info, vOffset, err
}

func readValue(meta MetaInfo, file *os.File) (interface{}, error) {
	buf := make([]byte, 5) // todo
	if _, err := file.ReadAt(buf, meta.Offset); err != nil {
		return nil, err
	}

	// Calculate offset for actual value
	vOffset, err := binary.ReadVarint(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	vOffset += 2

	buf = make([]byte, meta.VSize)
	if _, err := file.ReadAt(buf, meta.Offset+vOffset); err != nil {
		return nil, err
	}
	fmt.Println(buf)

	var v interface{}
	err = json.Unmarshal(buf, &v)
	return v, err
}
