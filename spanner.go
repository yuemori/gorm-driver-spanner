package gorm_driver_spanner

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"

	_ "github.com/yuemori/go-sql-driver-spanner"
)

type Dialector struct {
	DriverName string
	DSN        string
	Conn       gorm.ConnPool
}

func Open(dsn string) gorm.Dialector {
	return Dialector{DSN: dsn}
}

func (dialector Dialector) Name() string {
	return "spanner"
}

var (
	// CreateClauses create clauses
	CreateClauses = []string{"INSERT", "VALUES"}
	// UpdateClauses update clauses
	UpdateClauses = []string{"UPDATE", "SET", "WHERE", "ORDER BY", "LIMIT"}
	// DeleteClauses delete clauses
	DeleteClauses = []string{"DELETE", "FROM", "WHERE", "ORDER BY", "LIMIT"}
	// QueryClauses query clauses
	QueryClauses = []string{"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"}
)

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: CreateClauses,
		UpdateClauses: UpdateClauses,
		DeleteClauses: DeleteClauses,
		QueryClauses:  QueryClauses,
	})
	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open("spanner", dialector.DSN)
		if err != nil {
			return err
		}
	}
	return
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	panic("spanner driver does not support migrator now.")
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		return "INT64"
	case schema.Float:
		return "FLOAT64"
	case schema.String:
		return "STRING"
	case schema.Time:
		return "TIMESTAMP"
	case schema.Bytes:
		return "BYTES"
	}

	return string(field.DataType)
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	// make @v1, @v2, @v3...
	writer.WriteString(fmt.Sprintf("@v%d", len(stmt.Vars)))
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(".`")
			}
			writer.WriteString(str)
			writer.WriteByte('`')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('`')
	}
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return sql
}

func (dialectopr Dialector) SavePoint(tx *gorm.DB, name string) error {
	return errors.New("spanner does not support save point.")
}

func (dialectopr Dialector) RollbackTo(tx *gorm.DB, name string) error {
	return errors.New("spanner does not support save point.")
}
