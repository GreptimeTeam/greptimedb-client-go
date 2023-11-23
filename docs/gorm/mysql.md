Retrieving via MySQL
==


```go
package main

import (
	"fmt"
	"time"

	"gorm.io/driver/mysql"
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

type Mysql struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string

	DB *gorm.DB
}

func (m *Mysql) Setup() error {
	if m.DB != nil {
		return nil
	}

	dsn := fmt.Sprintf("tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		m.Host, m.Port, m.Database)
	if m.User != "" && m.Password != "" {
		dsn = fmt.Sprintf("%s:%s@%s", m.User, m.Password, dsn)
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	m.DB = db
	return nil
}

func (p *Mysql) AllMonitors() ([]Monitor, error) {
	var monitors []Monitor
	err := p.DB.Find(&monitors).Error
	return monitors, err
}

func main() {
	mysql := &Mysql{
		Host:     "127.0.0.1",
		Port:     "4002",
		User:     "",
		Password: "",
		Database: "public",
	}
	if err := mysql.Setup(); err != nil {
		panic(err)
	}

	all, err := mysql.AllMonitors()
	if err != nil {
		panic(err)
	}

	for i, m := range all {
		fmt.Printf("%d: %#v\n", i, m)
	}
}
```