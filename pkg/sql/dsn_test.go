package sql

import (
	"reflect"
	"testing"

	req "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

var testDSNs = []struct {
	in  string
	out *req.Config
}{{
	"username:password@protocol(address)/catalogname:dbname",
	&req.Config{UserName: "username", Password: "password", Net: "protocol", Address: "address", Catalog: "catalogname", Database: "dbname"},
}, {
	"username:password@protocol(address)/catalogname:",
	&req.Config{UserName: "username", Password: "password", Net: "protocol", Address: "address", Catalog: "catalogname", Database: ""},
}, {
	"username:password@protocol(address)/dbname",
	&req.Config{UserName: "username", Password: "password", Net: "protocol", Address: "address", Catalog: "", Database: "dbname"},
}, {
	"username:password@protocol(address)/",
	&req.Config{UserName: "username", Password: "password", Net: "protocol", Address: "address", Catalog: "", Database: ""},
}, {
	"username:password@protocol/dbname",
	&req.Config{UserName: "username", Password: "password", Net: "protocol", Address: "127.0.0.1:4001", Catalog: "", Database: "dbname"},
}, {
	"username:password@(address)/dbname",
	&req.Config{UserName: "username", Password: "password", Net: "", Address: "address", Catalog: "", Database: "dbname"},
}, {
	"username:password@/dbname",
	&req.Config{UserName: "username", Password: "password", Net: "", Address: "127.0.0.1:4001", Catalog: "", Database: "dbname"},
}, {
	"username@/dbname",
	&req.Config{UserName: "username", Password: "", Net: "", Address: "127.0.0.1:4001", Catalog: "", Database: "dbname"},
}, {
	":password@/dbname",
	&req.Config{UserName: "", Password: "password", Net: "", Address: "127.0.0.1:4001", Catalog: "", Database: "dbname"},
}, {
	"/dbname",
	&req.Config{UserName: "", Password: "", Net: "", Address: "127.0.0.1:4001", Catalog: "", Database: "dbname"},
}, {
	"/",
	&req.Config{Net: "", Address: "127.0.0.1:4001", Catalog: "", Database: ""},
}, {
	"",
	&req.Config{Net: "", Address: "127.0.0.1:4001", Catalog: "", Database: ""},
}, {
	"username:p@ssword@/dbname",
	&req.Config{UserName: "username", Password: "p@ssword", Net: "", Address: "127.0.0.1:4001", Catalog: "", Database: "dbname"},
}, {
	"/catalogname::::dbname",
	&req.Config{UserName: "", Password: "", Net: "", Address: "127.0.0.1:4001", Catalog: "catalogname", Database: ":::dbname"},
}, {
	"(protocal)(address)/dbname",
	&req.Config{UserName: "", Password: "", Net: "", Address: "protocal)(address", Catalog: "", Database: "dbname"},
},
}

func TestParseDSNToConfig(t *testing.T) {
	for i, tst := range testDSNs {
		cfg, err := ParseDSNToConfig(tst.in)
		if err != nil {
			t.Error(err.Error())
		}

		if !reflect.DeepEqual(cfg, tst.out) {
			t.Errorf("%d. ParseDSN(%q) mismatch:\ngot  %+v\nwant %+v", i, tst.in, cfg, tst.out)
		}
	}
}
