## rdb

Rdb is an extremely fast, key-value database insprised by bitcask.

Godoc: https://godoc.org/github.com/i/rdb

#### Features
1. Continuous snapshots (you can replay database from any point in time)
1. TODO

#### Operations
```lang=golang
Set(key string, value interface{})
Get(key string)
Delete(key string)
Keys()
```

