package main

import (
	"fmt"
	"reflect"
	"time"
	"unsafe"
)

type Monitor struct {
	Host   string    `db:"host,INDEX"`
	Ts     time.Time `db:"ts,TIMESTAMP"`
	Cpu    string    `db:"cpu"`
	Memory string    `db:"memory"`
}

func main() {
	s := []string{"hello"}
	fmt.Println(reflect.TypeOf(s).Kind())
	fmt.Println(reflect.TypeOf(s).Elem().Kind())

	b := []byte("hello")
	fmt.Println(reflect.TypeOf(b).Kind())
	fmt.Println(reflect.TypeOf(b).Elem().Kind())

	fmt.Println(reflect.TypeOf(time.Now()).Kind())
	fmt.Println(reflect.TypeOf(time.Now()).Name())

	fmt.Println(unsafe.Sizeof(uint64(1)))

	m := Monitor{
		Host:   "host value",
		Ts:     time.Now(),
		Cpu:    "cpu value",
		Memory: "mem value",
	}

	field := reflect.TypeOf(m).Field(0)
	val := reflect.ValueOf(field)
	fmt.Printf("%v\n", val)

	fmt.Printf("%v\n", reflect.ValueOf(m).Field(0).String())
}
