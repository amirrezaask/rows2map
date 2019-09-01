package row2map

import (
	"database/sql"
	"reflect"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

func convert(t reflect.Type, v reflect.Value) (interface{}, error) {
	switch t.String() {
	case "sql.RawBytes":
		return convertSliceOfUintToString(v.Interface().([]uint8)), nil
	case "mysql.NullTime":
		return v.Interface().(time.Time), nil
	case "sql.NullInt64":
		str := convertSliceOfUintToString(v.Interface().([]uint8))
		i, err := strconv.Atoi(str)
		if err != nil {
			return nil, errors.Wrap(err, "error while converting from int for string")
		}
		return i, nil
	default:
		return v.Interface(), nil
	}
}

func convertSliceOfUintToString(s []uint8) string {
	output := []rune{}
	final := ""
	_ = final
	for _, c := range s {
		o := rune(c)
		output = append(output, o)
	}
	for _, c := range output {
		final += string(c)
	}
	return string(output)
}

//ToMaps makes array of maps from rows
func ToMaps(rs *sql.Rows) ([]map[string]interface{}, error) {

	cols, _ := rs.Columns()
	cts, err := rs.ColumnTypes()
	_ = cts
	if err != nil {
		panic(err)
	}
	result := make([]map[string]interface{}, 0)
	for rs.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rs.Scan(columnPointers...); err != nil {
			panic(err)
		}

		m := make(map[string]interface{})
		for i, colName := range cols {
			val := *(columnPointers[i].(*interface{}))
			val, err = convert(cts[i].ScanType(), reflect.ValueOf(val))
			if err != nil {
				return nil, errors.Wrap(err, "error while converting from sql output to actual types")
			}
			m[colName] = val
		}

		result = append(result, m)
	}
	return result, nil
}
