// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"reflect"
	"testing"

	"github.com/lib/pq"
)

func TestSQLFilterBuilder(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var b sqlFilterBuilder
		b.addIncludeExclude("datname", nil, nil)

		where, args := b.whereClause()
		if where != "" || args != nil {
			t.Fatalf("whereClause() = (%q, %v), want (\"\", nil)", where, args)
		}
		and, args := b.andClause()
		if and != "" || args != nil {
			t.Fatalf("andClause() = (%q, %v), want (\"\", nil)", and, args)
		}
	})

	t.Run("include only", func(t *testing.T) {
		var b sqlFilterBuilder
		b.addIncludeExclude("datname", []string{"app", "orders"}, nil)

		where, args := b.whereClause()
		if want := "WHERE datname = ANY($1)"; where != want {
			t.Fatalf("whereClause() = %q, want %q", where, want)
		}
		wantArgs := []any{pq.Array([]string{"app", "orders"})}
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("args = %#v, want %#v", args, wantArgs)
		}
	})

	t.Run("exclude only", func(t *testing.T) {
		var b sqlFilterBuilder
		b.addIncludeExclude("pg_database.datname", nil, []string{"rdsadmin"})

		where, args := b.whereClause()
		if want := "WHERE pg_database.datname != ALL($1)"; where != want {
			t.Fatalf("whereClause() = %q, want %q", where, want)
		}
		wantArgs := []any{pq.Array([]string{"rdsadmin"})}
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("args = %#v, want %#v", args, wantArgs)
		}
	})

	t.Run("multiple columns with include and exclude", func(t *testing.T) {
		var b sqlFilterBuilder
		b.addIncludeExclude("pg_database.datname", []string{"app"}, []string{"rdsadmin"})
		b.addIncludeExclude("pg_get_userbyid(userid)", []string{"app_user"}, []string{"monitor"})

		and, args := b.andClause()
		want := "AND pg_database.datname = ANY($1) AND pg_database.datname != ALL($2) AND pg_get_userbyid(userid) = ANY($3) AND pg_get_userbyid(userid) != ALL($4)"
		if and != want {
			t.Fatalf("andClause() = %q, want %q", and, want)
		}
		wantArgs := []any{
			pq.Array([]string{"app"}),
			pq.Array([]string{"rdsadmin"}),
			pq.Array([]string{"app_user"}),
			pq.Array([]string{"monitor"}),
		}
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("args = %#v, want %#v", args, wantArgs)
		}
	})
}
