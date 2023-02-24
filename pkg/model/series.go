package model

import "time"

type Series struct {
	Name string
	Tags []Tag
	Vals []Value
	Time *time.Time
}

type Tag struct {
	Key string
	Val any
}

type Value struct {
	Key string
	Val any
}
