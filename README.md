
# naifdb: naive Key/Value store

This is a simple key value store following the basic ideas described in [Designing Data-Intensive Applications](https://dataintensive.net/) and articles I read.

This is an educational project, and has no other purpose. It will evolve it to use other techniques as I learn more about the theory.

# Design

The starting design is the one used by [Bitcask](https://en.wikipedia.org/wiki/Bitcask (default storage engine in Riak) ([paper](https://riak.com/assets/bitcask-intro.pdf)). Check the original design for trade-offs.

# Requirements

- go 1.15 or later is required

# Usage


```golang
store, err := NewStore()
defer store.Close()
```

```golang
err = store.Set([]byte("Key1"), []byte("Value 1"))
if err != nil {
    log.Println(err)
}
```

Get a value:

```golang
value, err := store.Get([]byte("Key1"))
if err != nil {
    log.Println(err)
}
```

# Author

* Duncan Mac-Vicar P.


# License

* MIT
