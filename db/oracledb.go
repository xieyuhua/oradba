/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sqloracle "github.com/wdrabbit/gorm-oracle"
	"gorm.io/gorm"
)

var ORA *sql.DB

const (
	DefaultQueryTimeout = 30 * time.Second
)

func NewOracleDBEngine(cfg *Cfg) (*sql.DB, error) {
	connString := fmt.Sprintf("oracle://%s:%s@%s",
		cfg.OracleDB.Username,
		cfg.OracleDB.Password,
		cfg.OracleDB.ConnectString)
	fmt.Println(connString)
	db, err := gorm.Open(sqloracle.Open(connString), &gorm.Config{})
	if err != nil {
		fmt.Println(err)
	}
	sqlDB, _ := db.DB()
	err = sqlDB.Ping()
	if err != nil {
		return sqlDB, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return sqlDB, nil
}

func Query(sql string) ([]string, [][]string, error) {
	var (
		columns []string
		vals    [][]string
	)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultQueryTimeout)
	defer cancel()

	rows, err := ORA.QueryContext(ctx, sql)
	if ctx.Err() != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return columns, vals, fmt.Errorf("sql query timeout [default 30s], please run again")
		}
		return columns, vals, ctx.Err()
	}
	if err != nil {
		return columns, vals, err
	}

	columns, err = rows.Columns()
	if err != nil {
		return columns, vals, fmt.Errorf("error on query rows.Columns failed: [%v]", err)
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return columns, vals, fmt.Errorf("error on query rows.Scan failed: [%v]", err)
		}

		var res []string
		for _, v := range values {
			res = append(res, string(v))
		}
		vals = append(vals, res)
	}
	return columns, vals, nil
}
