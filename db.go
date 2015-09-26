package rdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
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
}

type db struct {
	metaKeys map[string]MetaInfo
	files    map[string]*os.File
	cf       *os.File
	dirpath  string
	m        sync.Mutex
}

func NewDB(dirpath string) DB {
	db := &db{
		metaKeys: make(map[string]MetaInfo),
		files:    make(map[string]*os.File),
		dirpath:  dirpath,
	}
	db.replay()
	db.setCf()
	return db
}

type MetaInfo struct {
	Ts     time.Time
	Path   string
	Offset int64
	VSize  int
	MSize  int
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
		Ts:     time.Now(),
		Path:   path.Join(d.cf.Name()),
		Offset: offset,
		VSize:  len(vbuf),
		MSize:  MetaSize,
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

func newMetaBuffer(meta MetaInfo) ([]byte, error) {
	buf := make([]byte, MetaSize)
	m, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	for i, b := range m {
		buf[i] = b
	}
	return buf, nil
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

	buf := make([]byte, meta.VSize)
	_, err := file.ReadAt(buf, meta.Offset+MetaSize)
	if err != nil {
		return nil, err
	}

	var v interface{}
	if err := json.Unmarshal(buf, &v); err != nil {
		return nil, err
	}

	return v, nil
}

func (d *db) replay() {
	// todo
	// read all files in d.dirname
	// sort them// for each file:
	// 	 read meta
	//   load meta into metakeys
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
