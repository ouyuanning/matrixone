// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func Test_buildTestShowCreateTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "test1",
			sql: `CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT NOT NULL,
				store_id INT NOT NULL
			)`,
			want: "CREATE TABLE `employees` (\n  `id` int NOT NULL,\n  `fname` varchar(30) DEFAULT NULL,\n  `lname` varchar(30) DEFAULT NULL,\n  `hired` date NOT NULL DEFAULT '1970-01-01',\n  `separated` date NOT NULL DEFAULT '9999-12-31',\n  `job_code` int NOT NULL,\n  `store_id` int NOT NULL\n)",
		},
		{
			name: "test2",
			sql: `CREATE TABLE t_time (
				    id INT AUTO_INCREMENT PRIMARY KEY,
				    date_column DATE, 
				    time_column TIME(3),
				    datetime_column DATETIME(6), 
				    timestamp_column TIMESTAMP(6),
				    timestamp_column2 TIMESTAMP
				);`,
			want: "CREATE TABLE `t_time` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `date_column` date DEFAULT NULL,\n  `time_column` time(3) DEFAULT NULL,\n  `datetime_column` datetime(6) DEFAULT NULL,\n  `timestamp_column` timestamp(6) NULL DEFAULT NULL,\n  `timestamp_column2` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`id`)\n)",
		},
		{
			name: "test3",
			sql: `CREATE TABLE t_time (
				id INT AUTO_INCREMENT PRIMARY KEY,
				date_column DATE,
				time_column TIME(3) DEFAULT NULL,
				datetime_column DATETIME(6)  DEFAULT NULL,
				timestamp_column TIMESTAMP(6)  NULL DEFAULT NULL,
				timestamp_column2 TIMESTAMP  NULL DEFAULT NULL
				)`,
			want: "CREATE TABLE `t_time` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `date_column` date DEFAULT NULL,\n  `time_column` time(3) DEFAULT NULL,\n  `datetime_column` datetime(6) DEFAULT NULL,\n  `timestamp_column` timestamp(6) NULL DEFAULT NULL,\n  `timestamp_column2` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`id`)\n)",
		},
		{
			name: "test4",
			sql: `CREATE TABLE t_log (
				LOG_ID bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
				ROUND_ID bigint(20) UNSIGNED NOT NULL,
				USER_ID int(10) UNSIGNED NOT NULL,
				USER_IP int(10) UNSIGNED DEFAULT NULL,
				END_TIME datetime NOT NULL,
				USER_TYPE int(11) DEFAULT NULL,
				APP_ID int(11) DEFAULT NULL,
				PRIMARY KEY (LOG_ID,END_TIME),
				KEY IDX_EndTime (END_TIME),
				KEY IDX_RoundId (ROUND_ID),
				KEY IDX_UserId_EndTime (USER_ID,END_TIME)
				)`,
			want: "CREATE TABLE `t_log` (\n  `LOG_ID` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `ROUND_ID` bigint unsigned NOT NULL,\n  `USER_ID` int unsigned NOT NULL,\n  `USER_IP` int unsigned DEFAULT NULL,\n  `END_TIME` datetime NOT NULL,\n  `USER_TYPE` int DEFAULT NULL,\n  `APP_ID` int DEFAULT NULL,\n  PRIMARY KEY (`LOG_ID`,`END_TIME`),\n  KEY `idx_endtime` (`END_TIME`),\n  KEY `idx_roundid` (`ROUND_ID`),\n  KEY `idx_userid_endtime` (`USER_ID`,`END_TIME`)\n)",
		},
		{
			name: "test5",
			sql: `CREATE TABLE example_table (
				id INT NOT NULL,
				name VARCHAR(255) NOT NULL DEFAULT 'default_name',
				email VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id),
				UNIQUE KEY uk1 (name),
				UNIQUE KEY uk2 (email)
				);`,
			want: "CREATE TABLE `example_table` (\n  `id` int NOT NULL,\n  `name` varchar(255) NOT NULL DEFAULT 'default_name',\n  `email` varchar(255) DEFAULT NULL,\n  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP(),\n  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uk1` (`name`),\n  UNIQUE KEY `uk2` (`email`)\n)",
		},
		{
			name: "test6",
			sql: `create table t (
				a timestamp not null default current_timestamp,
				b timestamp(3) default current_timestamp(3),
				c datetime default current_timestamp,
				d datetime(4) default current_timestamp(4),
				e varchar(20) default 'cUrrent_tImestamp',
				f datetime(2) default current_timestamp(2) on update current_timestamp(2),
				g timestamp(2) default current_timestamp(2) on update current_timestamp(2),
				h date default '2024-01-01'
				)`,
			want: "CREATE TABLE `t` (\n  `a` timestamp NOT NULL DEFAULT current_timestamp(),\n  `b` timestamp(3) DEFAULT current_timestamp(3),\n  `c` datetime DEFAULT current_timestamp(),\n  `d` datetime(4) DEFAULT current_timestamp(4),\n  `e` varchar(20) DEFAULT 'cUrrent_tImestamp',\n  `f` datetime(2) DEFAULT current_timestamp(2) ON UPDATE current_timestamp(2),\n  `g` timestamp(2) DEFAULT current_timestamp(2) ON UPDATE current_timestamp(2),\n  `h` date DEFAULT '2024-01-01'\n)",
		},
		{
			name: "test7",
			sql: `CREATE TABLE src (id bigint NOT NULL,
				json1 json DEFAULT NULL,
				json2 json DEFAULT NULL,
				PRIMARY KEY (id),
				FULLTEXT(json1) WITH PARSER json,
				FULLTEXT(json1,json2) WITH PARSER json)`,
			want: "CREATE TABLE `src` (\n  `id` bigint NOT NULL,\n  `json1` json DEFAULT NULL,\n  `json2` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n FULLTEXT (`json1`) WITH PARSER json,\n FULLTEXT (`json1`,`json2`) WITH PARSER json\n)",
		},
		{
			name: "test8",
			sql: `CREATE TABLE src (id bigint NOT NULL,
                                json1 json DEFAULT NULL,
                                json2 json DEFAULT NULL,
                                PRIMARY KEY (id),
                                FULLTEXT idx01(json1) WITH PARSER json,
                                FULLTEXT idx02(json1,json2) WITH PARSER json)`,
			want: "CREATE TABLE `src` (\n  `id` bigint NOT NULL,\n  `json1` json DEFAULT NULL,\n  `json2` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n FULLTEXT `idx01`(`json1`) WITH PARSER json,\n FULLTEXT `idx02`(`json1`,`json2`) WITH PARSER json\n)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildTestShowCreateTable(tt.sql)
			if err != nil {
				t.Fatalf("test name:%v, err: %+v, sql=%v", tt.name, err, tt.sql)
			}

			if tt.want != got {
				t.Errorf("buildShow failed. \nExpected/Got:\n%s\n%s", tt.want, got)
			}
		})
	}
}

func Test_SingleShowCreateTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "test7",
			sql: `CREATE TABLE example_table (
				id INT NOT NULL,
				name VARCHAR(255) NOT NULL DEFAULT 'default_name',
				email VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id),
				UNIQUE KEY uk1 (name),
				UNIQUE KEY uk2 (email)
				);`,
			want: "CREATE TABLE `example_table` (\n  `id` int NOT NULL,\n  `name` varchar(255) NOT NULL DEFAULT 'default_name',\n  `email` varchar(255) DEFAULT NULL,\n  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP(),\n  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uk1` (`name`),\n  UNIQUE KEY `uk2` (`email`)\n)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildTestShowCreateTable(tt.sql)
			if err != nil {
				t.Fatalf("test name:%v, err: %+v, sql=%v", tt.name, err, tt.sql)
			}

			if tt.want != got {
				t.Errorf("buildShow failed. \nExpected/Got:\n%s\n%s", tt.want, got)
			}
		})
	}
}

func buildTestCreateTableStmt(opt Optimizer, sql string) (*TableDef, error) {
	statements, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		return nil, err
	}
	// this sql always return single statement
	context := opt.CurrentContext()
	ddlPlan, err := BuildPlan(context, statements[0], false)
	//if ddlPlan != nil {
	//	testDeepCopy(ddlPlan)
	//}

	planDdl := ddlPlan.Plan.(*plan.Plan_Ddl)
	definition := planDdl.Ddl.Definition.(*plan.DataDefinition_CreateTable)
	definition.CreateTable.TableDef.TableType = catalog.SystemOrdinaryRel

	return definition.CreateTable.TableDef, err
}

func buildTestShowCreateTable(sql string) (string, error) {
	mock := NewMockOptimizer(false)
	tableDef, err := buildTestCreateTableStmt(mock, sql)
	if err != nil {
		return "", err
	}

	var snapshot *plan.Snapshot
	showSQL, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, snapshot, false)
	if err != nil {
		return "", err
	}
	return showSQL, nil
}
