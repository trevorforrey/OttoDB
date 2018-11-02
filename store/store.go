package store

type store interface {
	Get() string
	Set() string
	Del() string
}
