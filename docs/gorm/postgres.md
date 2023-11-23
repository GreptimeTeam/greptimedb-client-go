Retrieving via PostgresQL
==

```go
package main

import (
	"fmt"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Monitor struct {
	ID          int64     `gorm:"primaryKey"`
	Host        string    `gorm:"column:host"`
	Memory      uint64    `gorm:"column:memory"`
	Cpu         float64   `gorm:"column:cpu"`
	Temperature int64     `gorm:"column:temperature"`
	Ts          time.Time `gorm:"column:ts"`
}

func (Monitor) TableName() string {
	return "monitor"
}

type Postgres struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string

	DB *gorm.DB
}

// Setup is to init the DB, and SHOULD BE called only once
func (p *Postgres) Setup() error {
	if p.DB != nil {
		return nil
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=Asia/Shanghai",
		p.Host, p.User, p.Password, p.Database, p.Port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	p.DB = db
	return nil
}

func (p *Postgres) AllMonitors() ([]Monitor, error) {
	var monitors []Monitor
	err := p.DB.Find(&monitors).Error
	return monitors, err
}

func main() {
	pg := &Postgres{
		Host:     "127.0.0.1",
		Port:     "4003",
		User:     "",
		Password: "",
		Database: "public",
	}
	if err := pg.Setup(); err != nil {
		panic(err)
	}

	all, err := pg.AllMonitors()
	if err != nil {
		panic(err)
	}
	for i, m := range all {
		fmt.Printf("%d: %#v\n", i, m)
	}
}
```