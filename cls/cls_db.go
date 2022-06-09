package cls

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"github.com/go-sql-driver/mysql"
	"github.com/labstack/echo/v4"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/go-camelcase"
)

var Dbbd string   // dbms name - mssql, mysql
var Dbid string   // dbms id
var Dbps string   // dbms password
var Dbpt string   // dbms port
var Dbnm string   // dbms instance
var Dbssl int     // dbms ssl encrypt option
var DbTimeOut int // dbms timeout
var DBAgentPort string

var DBc *sql.DB
var DBRand *rand.Rand
var ConnIdx int     // 현재 접속중인 ip 배열 index - 최초는 0번째 ip로 연결
var ConnLen int     // 접속할 ip 갯수
var Dbip []string   // dbms ip
var DbipList string // dbms ip list

var DBMutex sync.RWMutex

// resut for more viewing
type RespDbms struct {
	Result   int
	EventYn  string
	RowCount int
}

// database error
type DbErr struct {
	Level   string
	Code    int
	Message string
}

const (
	DBCONN_ERR     = 1 // db 접속 변경
	DBCONN_CONTI       // db 접속 유지
	DBCONN_DECLINE     // db 응답 느림
	DBETC_ERR          // 이 외의 에러인 경우
)

// 일반적인 SELECT 쿼리 시 사용 -결과 string
func GetSelectData(sqlQuery string, params map[string]string, c echo.Context) ([]map[string]string, error) {

	//params := GetParamJsonMap(c)
	var errMap []map[string]string
	// 파라메터 맵으로 쿼리 변환
	selectQuery, err := SetUpdateParam(sqlQuery, params)
	if err != nil {
		Lprintf(4, "[INFO] %d call GetQueryJson \n", err)
		return errMap, err
	}
	// 쿼리 실행 후 JSON 형태로 결과 받기

	resultData, err := QueryMapColumn(selectQuery, c)
	if err != nil {
		Lprintf(4, "[INFO] %d call QueryMapColumn \n", err)
		return errMap, err
	}

	return resultData, nil
}

func GetSelectDataRequire(sqlQuery string, params map[string]string, c echo.Context) ([]map[string]string, error) {

	//params := GetParamJsonMap(c)
	var errMap []map[string]string
	// 파라메터 맵으로 쿼리 변환
	selectQuery, err := GetQueryJson(sqlQuery, params)
	if err != nil {
		Lprintf(4, "[INFO] %d call GetQueryJson \n", err)
		return errMap, err
	}
	// 쿼리 실행 후 JSON 형태로 결과 받기

	resultData, err := QueryMapColumn(selectQuery, c)
	if err != nil {
		Lprintf(4, "[INFO] %d call QueryMapColumn \n", err)
		return errMap, err
	}

	return resultData, nil
}

// 일반적인 SELECT 쿼리 시 Json 사용 -결과 string
func GetSelectDataUsingJson(sqlQuery string, params map[string]string, c echo.Context) ([]map[string]string, error) {

	//params := GetParamJsonMap(c)
	var errMap []map[string]string
	// 파라메터 맵으로 쿼리 변환
	selectQuery, err := GetQueryJson(sqlQuery, params)
	if err != nil {
		Lprintf(4, "[INFO] %d call GetQueryJson \n", err)
		return errMap, err
	}
	// 쿼리 실행 후 JSON 형태로 결과 받기

	resultData, err := QueryMapColumn(selectQuery, c)
	if err != nil {
		Lprintf(4, "[INFO] %d call QueryMapColumn \n", err)
		return errMap, err
	}

	return resultData, nil
}

// 일반적인 SELECT 쿼리 시 사용 - 결과 type 별
func GetSelectType(sqlQuery string, params map[string]string, c echo.Context) ([]interface{}, error) {

	//params := GetParamJsonMap(c)
	var errMap []interface{}
	// 파라메터 맵으로 쿼리 변환
	selectQuery, err := SetUpdateParam(sqlQuery, params)
	if err != nil {
		Lprintf(4, "[INFO] %d call GetQueryJson \n", err)
		return errMap, err
	}
	// 쿼리 실행 후 JSON 형태로 결과 받기

	resultData, err := QueryList(selectQuery, c)
	if err != nil {
		Lprintf(4, "[INFO] %d call QueryMapColumn \n", err)
		return errMap, err
	}

	return resultData, nil
}


func GetSelectTypeRequire(sqlQuery string, params map[string]string, c echo.Context)([]interface{}, error) {

	//params := GetParamJsonMap(c)
	var errMap []interface{}
	// 파라메터 맵으로 쿼리 변환
	selectQuery, err := GetQueryJson(sqlQuery, params)
	if err != nil {
		Lprintf(4, "[INFO] %d call GetQueryJson \n", err)
		return errMap, err
	}
	// 쿼리 실행 후 JSON 형태로 결과 받기

	resultData, err := QueryList(selectQuery, c)
	if err != nil {
		Lprintf(4, "[INFO] %d call QueryMapColumn \n", err)
		return errMap, err
	}

	return resultData, nil
}


// web request 에서 Query의 #{}과 일치하는 parmaeter를 치환하여 update, insert 사용
// 쿼리에 #{XXX_NULL}(NULL로 끝나는 경우)로 셋팅하면 request 파라메터가 없으면 'NULL'로 update
func SetUpdateParam(query string, params map[string]string) (string, error) {
	lines := strings.Split(SetPreProcess(query, params), "\n")

	// every line
	lineQuery := ""
	newQuery := ""
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if strings.Index(line, "SET") >= 0 || strings.Index(line, "AND") >= 0 || strings.Index(line, "WHERE") >= 0 || strings.Index(line, "VALUES") >= 0 {
			i, lineQuery = setSetQuery(i, lines, params)
			line = lineQuery
		}
		if i < 0 {
			return "", errors.New(line)
		}
		newQuery = newQuery + line + "\n"
	}
	return newQuery, nil
}

// json 파라메터를 쿼리에 매핑하는 함수
func GetQueryJson(query string, params map[string]string) (string, error) {
	Lprintf(4, "call Query (%s)\n", query)
	for k, v := range params {
		query = strings.ReplaceAll(query, "#{"+k+"}", v)
	}

	if idx := strings.Index(query, "#{"); idx >= 0 {
		Lprintf(1, "not enough parameter error : %s\n", query[idx:])
		return "1100", errors.New("not enough parameter")
	}

	return query, nil
}

// select 쿼리를 날려서  Map으로 만들어서 리턴하는  함수
func QueryMapColumn(query string, c echo.Context) ([]map[string]string, error) {


	DBMutex.RLock()
	//Dprintf(4, c, "call Query lock (%s)\n", query)
	rows, dberr := DBc.Query(query)
	Dprintf(4, c, "call Query finish (%s)\n", query)
	DBMutex.RUnlock()
	defer rows.Close()
	if dberr != nil {
		Dprintf(1, c, "(%s) call error : %s\n", query, dberr)
		return nil, dberr
	}

	var resultMap []map[string]string
	// 응답코드
	cols, _ := rows.Columns()
	for i := 0; rows.Next(); i++ {
		resultOne := make(map[string]string)

		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		for i, _ := range cols {
			val := columnPointers[i].(*interface{})
			value := fmt.Sprintf("%s", *val)
			resultOne[cols[i]] = value
		}
		resultMap = append(resultMap, resultOne)
	}

	return resultMap, nil
}

// select 쿼리를 날려서 리스트([]interface{}) 를 만들어서 리턴하는 함수
func QueryList(query string, c echo.Context) ([]interface{}, error) {
	Dprintf(4, c, "call Query (%s)\n", query)
	DBMutex.RLock()
	rows, dbErr := DBc.Query(query)
	DBMutex.RUnlock()
	defer rows.Close()
	if dbErr != nil {
		Dprintf(1, c, "(%s) call error : %s\n", query, dbErr)
		return nil, dbErr
	}

	// 데이터 타입 구분을 위해 column 배열을 가져옴
	columns, err := rows.ColumnTypes()
	if err != nil {
		Dprintf(1, c, "(%s) ColumnTypes error : %s\n", query, err)
		return nil, err
	}

	// 쿼리 결과로 반환할 리스트
	var finalRows []interface{}

	for rows.Next() {
		scanArgs := make([]interface{}, len(columns))
		// 컬럼의 데이터베이스 타입을 기준으로 객체 할당
		for i, col := range columns {
			switch col.DatabaseTypeName() {
			case "BOOL", "BOOLEAN", "TINYINT":
				scanArgs[i] = new(sql.NullBool)
				break
			case "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "SMALLINT", "MEDIUMINT", "BIGINT" , "UNSIGNED BIGINT":
				scanArgs[i] = new(sql.NullInt64)
				break
			case "FLOAT", "DECIMAL", "DOUBLE":
				scanArgs[i] = new(sql.NullFloat64)
				break
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			Dprintf(1, c, "(%s) row scan error : %s\n", query, err)
			return nil, err
		}

		// 한 row 의 데이터
		rowData := make(map[string]interface{})
		// rowData mapping
		for i, c := range columns {
			columnName := camelcase.Camelcase(c.Name())
			value := scanArgs[i]
			if v, ok := value.(*sql.NullString); ok {
				rowData[columnName] = v.String
				continue
			}
			if v, ok := value.(*sql.NullInt64); ok {
				rowData[columnName] = v.Int64
				continue
			}
			if v, ok := value.(*sql.NullBool); ok {
				rowData[columnName] = v.Bool
				continue
			}
			if v, ok := value.(*sql.NullFloat64); ok {
				rowData[columnName] = v.Float64
				continue
			}
			if v, ok := value.(*sql.NullInt32); ok {
				rowData[columnName] = v.Int32
				continue
			}
			rowData[columnName] = value
		}

		finalRows = append(finalRows, rowData)
	}

	return finalRows, nil
}

// select 쿼리를 날려서 한개의 데이터(interface{}) 를 만들어서 리턴하는 함수
func QueryData(query string, c echo.Context) (interface{}, error) {
	list, err := QueryList(query, c)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		Dprintf(4, c, "(%s) query result is empty : %s\n", query, nil)
		return nil, fmt.Errorf("query result is empty : %s", query)
	}
	return list[0], nil
}

// -------------------- 내부 함수 ----------------------------
// if 문 처리를 위하 query 전처리
func SetPreProcess(query string, params map[string]string) string {
	result := ""
	lines := strings.Split(query, "\n")
	for _, line := range lines {
		line = strings.TrimLeft(line, " \t") // 공백제거
		if strings.HasPrefix(line, "if ") {  // if 문 발견 -> 문장 분해
			line = ParseIfLine(line, params)
			if line == "" { // 빈줄인 경우 skip
				continue
			}
		}
		// 괄호를 닫는 경우 마지막 리스트의 , 를 제거해 준다.
		if strings.TrimSpace(line) == ")" {
			result = strings.TrimRight(result, ", \n") + "\n"
		}
		result = result + line + "\n"
	}
	return result
}

// if 문 해석
func ParseIfLine(line string, params map[string]string) string {
	token := strings.Split(line, " ")

	if !strings.HasPrefix(token[1], "#{") {
		Lprintf(1, "query if format error : %s\n", line)
		return line
	}

	// if 키
	key := token[1][2 : len(token[1])-1]
	Lprintf(4, "query key: %s\n", key)

	// if 조건
	condition := false
	if token[2] == "==" {
		condition = true
	}

	// if 값
	value := strings.Trim(token[3], "'")

	// request 값
	param := params[key]

	// then 구문 확인
	if token[4] != "then" {
		Lprintf(1, "query if then format error : %s\n", line)
		return line
	}

	// then 과 else 절 확인
	elseFlag := false
	elseQuery := ""
	thenQuery := ""
	for i := 5; i < len(token); i++ {
		if token[i] == "else" {
			elseFlag = true
			continue
		}
		if elseFlag {
			elseQuery = elseQuery + token[i] + " "
		} else {
			thenQuery = thenQuery + token[i] + " "
		}
	}

	// 조건이 맞으면 then
	if (param == value && condition) || (param != value && !condition) {
		return thenQuery
	}
	// 안 맞으면 else
	return elseQuery
}

// Insert나 Update의 쿼리를 파라미터 기준으로 조정한다.
func setSetQuery(sIdx int, lines []string, params map[string]string) (int, string) {
	newQuery := lines[sIdx] + "\n"
	for sIdx = sIdx + 1; sIdx < len(lines); sIdx++ {
		line := lines[sIdx]

		// where 절 만나면 종료
		if strings.Index(line, "WHERE") >= 0 {
			sIdx = sIdx - 1 // rewind
			break
		}

		// find variable
		if idx := strings.Index(line, "#{"); idx >= 0 {
			fidx := strings.Index(line[idx:], "}")
			if fidx < 0 {
				Lprintf(1, "query format error : %s\n", line[idx:])
				return -1, "query format error"
			}

			// 쿼리에서 parameter 이름 추출 및 NULL 허용 여부 check
			param := line[idx+2 : idx+fidx]
			isNull := false
			if strings.HasSuffix(param, "_NULL") {
				isNull = true
				param = param[:len(param)-5]
			}

			// parameter 이름에 맞는 value로 치환
			value := params[param]
			if value == "" {
				if isNull { // NULL 허용이면 NULL 로 SET
					Lprintf(4, "get param but is null(%s)\n", param)
					line = strings.ReplaceAll(line, "#{"+param+"_NULL}", "NULL")
				} else { // NULL 미허용이면 SET 하지 않음
					Lprintf(4, "skip line(%s) param not setted %s\n", line)
					continue
				}
			} else {
				if isNull { // NULL 허용인 경우 쿼리의 param으로 다시 변경
					param = param + "_NULL"
				}
				Lprintf(4, "set param(%s) to value(%s)\n", param, value)
				line = strings.ReplaceAll(line, "#{"+param+"}", value)
			}
		}
		// 괄호를 닫는 경우 마지막 리스트의 , 를 제거해 준다.
		if strings.TrimSpace(line) == ")" {
			newQuery = strings.TrimRight(newQuery, ", \n") + "\n"
		}
		newQuery = newQuery + line + "\n"
	}
	return sIdx, strings.TrimRight(newQuery, ", \n")
}

//----------------------- 절취선 -------------------------------
// select 쿼리를 날려서 JSON 형태(칼럼 리스트)로 만들어서 리턴하는  함수
func QueryJsonColumn(query string, c echo.Context) ([]byte, error) {

	Dprintf(4, c, "call Query (%s)\n", query)
	DBMutex.RLock()
	rows, dberr := DBc.Query(query)
	DBMutex.RUnlock()
	if dberr != nil {
		Dprintf(1, c, "(%s) call error : %s\n", query, dberr)
		return []byte("1001"), dberr
	}
	defer rows.Close()

	// 응답코드
	cols, _ := rows.Columns()
	resultList := ""
	rowCnt := 0
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return []byte("1002"), err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		// m := make(map[string]interface{})
		resultRow := ""
		for i, _ := range cols {
			val := columnPointers[i].(*interface{})
			value := fmt.Sprintf("%s", *val)
			elem := fmt.Sprintf("\"%s\":\"%s\",", cols[i], value)
			if value == "true" || value == "false" {
				elem = fmt.Sprintf("\"%s\":%s,", cols[i], value)
			}
			resultRow = resultRow + elem
		}
		resultRow = "{" + resultRow[:len(resultRow)-1] + "},"
		resultList = resultList + resultRow
		rowCnt++
	}

	resultList = resultList[:len(resultList)-1] + ""
	if rowCnt > 1 {
		resultList = "[" + resultList + "]"
	}
	return []byte(resultList), nil
}

// Select 쿼리를 날려서 결과를 JSON 바이트 형태로 받는 함수
func GetSelect(query string, c echo.Context) ([]byte, error) {

	Dprintf(4, c, "call (%s)\n", query)

	DBMutex.RLock()
	rows, dberr := DBc.Query(query)
	DBMutex.RUnlock()
	if dberr != nil {
		Dprintf(1, c, "%s call error : %s\n", query, dberr)
		return []byte("1001"), dberr
	}
	defer rows.Close()

	// 응답코드
	cols, _ := rows.Columns()
	resultList := "["
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return []byte("1002"), err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		// m := make(map[string]interface{})
		resultRow := ""
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			// m[colName] = *val
			elem := fmt.Sprintf("\"%s\":\"%s\",", colName, *val)
			resultRow = resultRow + elem
		}
		resultRow = "{" + resultRow[:len(resultRow)-1] + "},"
		resultList = resultList + resultRow
	}
	resultList = resultList[:len(resultList)-1] + "]"
	return []byte(resultList), nil
}

// JSON 스트링에서 해당 키로 결과를 추출
func GetResultValue(json string, key string) (string, error) {
	first := strings.Index(json, key)
	if first < 0 {
		Lprintf(1, "[ERR ] not found key : (%s)\n", key)
		return "5000", errors.New("not found key")
	}
	idx := strings.Index(json[first:], ":")
	if idx < 0 {
		Lprintf(1, "[ERR ] not found key : (%s)\n", key)
		return "5000", errors.New("format error json")
	}
	idx += first
	sta := strings.Index(json[idx:], "\"")
	if sta < 0 {
		Lprintf(1, "[ERR ] not found key : (%s)\n", key)
		return "5000", errors.New("not found start")
	}
	sta += 1 + idx
	fin := strings.Index(json[sta:], "\"")
	if fin < 0 {
		Lprintf(1, "[ERR ] not found key : (%s)\n", key)
		return "5000", errors.New("not found finish")
	}
	fin += sta

	return json[sta:fin], nil
}

// web request 에서 Query의 #{}과 일치하는 parmaeter를 치환하여 select 쿼리하는 함수
func SetQueryParam(query string, c echo.Context) (string, error) {

	paramList, err := c.FormParams()
	if err != nil {
		return "", err
	}

	for k, a := range paramList {
		query = strings.ReplaceAll(query, "#{"+k+"}", a[0])
	}

	if idx := strings.Index(query, "#{"); idx >= 0 {
		Dprintf(1, c, "not enough parameter error : %s\n", query[idx:])
		return "1100", errors.New("not enough parameter")
	}

	return query, nil
}

// db transaction (여러 쿼리 가능)
func QueryDBTrans(querys []string, c echo.Context) (string, error) {
	// transation begin
	tx, err := DBc.Begin()
	if err != nil {
		return "5100", errors.New("begin error")
	}

	// 오류 처리
	defer func() {
		if err != nil {
			// transaction rollback
			Dprintf(4, c, "do rollback \n")
			tx.Rollback()
		}
	}()

	// transation exec
	for _, query := range querys {
		_, err = tx.Exec(query)
		if err != nil {
			Dprintf(1, c, "Query(%s) -> error (%s) \n", query, err)
			return "1000", err
		}
	}

	// transaction commit
	err = tx.Commit()
	if err != nil {
		return "5101", err
	}
	return "0000", nil
}

// DBMS retry 기능이 들어간 쿼리(Select)
func QueryDB(query string, params ...interface{}) (*sql.Rows, error) {
	if DBc == nil {
		time.Sleep(10 * time.Millisecond)
	}

	DBMutex.RLock()
	rows, err := DBc.Query(query, params...)
	DBMutex.RUnlock()
	defer rows.Close()

	Lprintf(4, "[INFO] query resp string : %s\n", query)
	Lprintf(4, "[INFO] params resp string : %s\n", params)
	if err != nil {
		Lprintf(1, "[ERR ] Query error : %s \n", err)
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no route to host") {
			DbmsDuplexing()
		}
		// try one more
		DBMutex.RLock()
		defer DBMutex.RUnlock()
		return DBc.Query(query, params...)
	}
	return rows, err
}

// DBMS retry 기능이 들어간 쿼리(Insert, Update, Delete)
func ExecDB(query string, params ...interface{}) (int,int64,error) {
	if DBc == nil {
		time.Sleep(10 * time.Millisecond)
	}

	DBMutex.RLock()
	ret, err := DBc.Exec(query, params...)
	DBMutex.RUnlock()

	Lprintf(4, "[INFO] query resp string : %s\n", query)
	Lprintf(4, "[INFO] params resp string : %s\n", params)
	if err != nil {
		Lprintf(1, "[ERR ] Query error : %s \n", err)
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no route to host") {
			DbmsDuplexing()

			// try one more
			DBMutex.RLock()
			ret, err = DBc.Exec(query, params...)
			DBMutex.RUnlock()

			if err != nil {
				Lprintf(1, "[ERR ] Query error : %s \n", err)
				return 0,0, err
			}

			n, err := ret.RowsAffected()

			Lprintf(4, "[INFO] %d row affected \n", n)
			return int(n),0, err
		}
		return 0,0, err
	}

	// sql.Result.RowsAffected() 체크
	n, err := ret.RowsAffected()
	seq,_ := ret.LastInsertId()
	Lprintf(4, "[INFO] %d row affected \n", n)
	return int(n),seq,err
}

// db 설정
func Db_conf(fname string) int {
	Lprintf(4, "[INFO] conf start (%s)\n", fname)

	// dbms connect address
	/*
		dbip, r := GetTokenValue("HOST_DBMS", fname)
		if r == CONF_ERR {
			Lprintf(1, "[FAIL] DBMS not exist value\n")
			return (-1)
		}
	*/

	DBMutex.Lock()
	defer DBMutex.Unlock()

	dbip, r := GetTokenValue("HOST_DBMS", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS not exist value\n")
		return (-1)
	}
	DbipList = dbip
	Dbip = strings.Split(dbip, "&")
	ConnLen = len(Dbip)
	Lprintf(4, "[INFO] Dbip : (%s)\n", Dbip)

	s1 := rand.NewSource(time.Now().UnixNano())
	DBRand = rand.New(s1)
	ConnIdx = DBRand.Intn(ConnLen)

	// dbms connect id
	dbid, r := GetTokenValue("ID_DBMS", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_ID not exist value\n")
		return (-1)
	}
	Dbid = dbid
	Lprintf(4, "[INFO] Dbid : (%s)\n", Dbid)

	// dbms connect password
	dbps, r := GetTokenValue("PASSWD_DBMS", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_PASS not exist value\n")
		return (-1)
	}
	Dbps = dbps
	// Lprintf(4, "[INFO] Dbps : (%s)\n", Dbps)

	// dbms instance name
	dbnm, r := GetTokenValue("NAME_DBMS", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_DB not exist value\n")
		return (-1)
	}
	Dbnm = dbnm
	Lprintf(4, "[INFO] Dbnm : (%s)\n", Dbnm)

	// dbms instance name
	dbpt, r := GetTokenValue("PORT_DBMS", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_DB not exist value\n")
		return (-1)
	}
	Dbpt = dbpt
	Lprintf(4, "[INFO] Dbpt : (%s)\n", Dbpt)

	// dbms brand
	dbbd, r := GetTokenValue("DBMS", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_DB not exist value\n")
		dbbd = "mysql"
	}
	Dbbd = dbbd
	Lprintf(4, "[INFO] Dbbd : (%s)\n", Dbbd)

	dbssl, r := GetTokenValue("SSL_DBMS", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] SSL_DBMS not exist value\n")
	}

	if dbssl == "Y" {
		Dbssl = 1
		Lprintf(4, "[INFO] DBMS SSL ENCRYPT USE \n")
	}

	// dbms timeout
	dbTimeout, r := GetTokenValue("TIMEOUT_DBMS", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] TIMEOUT_DBMS not exist value, so set 10 seconds\n")
		dbTimeout = "10" // db query timeout config에 미설정시 default 값으로 10초 설정
		//return (-1)
	}
	DbTimeOut, _ = strconv.Atoi(dbTimeout)
	Lprintf(4, "[INFO] DbTimeOut : %d\n", DbTimeOut)

	DBc, _ = Initdb(dbbd, dbid, dbps, Dbip[ConnIdx], dbpt, dbnm)

	if DBc == nil {
		return (-1)
	}

	go CheckDbmsLive()

	return 0
}

// db에 재 접속한다.
func DbmsReinit() {

	newIdx := DBRand.Intn(ConnLen)
	Lprintf(4, "[INFO] DBMS connection change next (%d) \n", newIdx)
	if err := DBc.Close(); err != nil { // 기존 DB CLOSE
		Lprintf(1, "[ERR ] DBc close is error : [%s]\n", err)
		return
	}

	c, err := Initdb(Dbbd, Dbid, Dbps, Dbip[newIdx], Dbpt, Dbnm)
	if err != nil {
		Lprintf(1, "[ERR ] DBc init error : [%s]\n", err)
		return
	}
	DBc = c
	ConnIdx = newIdx
	return
}

// config에 등록된 다른 DBMS로 연결한다.
func DbmsDuplexing() {
	Lprintf(4, "[INFO] DBMS connection change now (%s) ---> next \n", Dbip[ConnIdx])
	if err := DBc.Close(); err != nil { // 기존 DB CLOSE
		Lprintf(1, "[ERR ] DBc close is error : [%s]\n", err)
	}

	newIdx := (ConnIdx + 1) % ConnLen
	c, err := Initdb(Dbbd, Dbid, Dbps, Dbip[newIdx], Dbpt, Dbnm)
	DBc = c
	if err == nil {
		ConnIdx = newIdx
	}
	return
}

// dbms iplist change
func ChangeDBMS(iplist string) bool {
	if iplist == DbipList {
		return false
	}

	DBMutex.Lock()
	Dbip = strings.Split(iplist, "&")
	ConnLen = len(Dbip)
	DbmsReinit()
	DBMutex.Unlock()

	return true
}

// DBMS 헬스체크 - 10 초에 한번
func CheckDbmsLive() {
	Lprintf(4, "[INFO] check db connection start \n")
	fctl := "/smartagent/Plugins/DFA/smartagent/tmp/scale.ctl"
	fname := "/smartagent/Plugins/DFA/smartagent/tmp/scale.data"
	for {
		time.Sleep(10 * time.Second)
		err := DBc.Ping()

		if err != nil {
			Lprintf(4, "[INFO] check db ping error (%s)\n", err)

			DBMutex.Lock()
			DbmsDuplexing()
			DBMutex.Unlock()
		}

		// db ip check
		if _, err := os.Stat(fctl); os.IsNotExist(err) {
			continue
		}
		os.Remove(fctl)
		v, r := GetTokenValue("DBIP", fname) // value & return
		Lprintf(1, "[INFO] DB change value : %s \n", v)
		if r != CONF_ERR {
			ChangeDBMS(v)
		}
		os.Remove(fname)
	}
}

// DBMS Timeout 기능이 들어간 쿼리
// cancel() - Canceling this context releases resources associated with it, so code should call cancel as soon as the operations running in this Context complete
func QueryTimeOut(query string) (*sql.Rows, uint, context.CancelFunc) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(DbTimeOut))
	rows, err := DBc.QueryContext(ctx, query)

	//Lprintf(4, "[INFO] query resp string : %s\n", err)
	if err != nil {
		Lprintf(1, "[ERR ] QueryContext : %s \n", err)

		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no route to host") {
			// 서버 접속이 안됨으로 판단하고 DB Session 교체
			DbmsDuplexing()
			Lprintf(4, "[INFO] DBMS down, so changed\n")
			return nil, DBCONN_ERR, cancel

		} else if strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "connection timed out") {
			//err = DBc.ping()
			//if err != nil {
			DbmsDuplexing()
			Lprintf(4, "[INFO] DBMS down(%s), so changed\n", err.Error())
			return nil, DBCONN_ERR, cancel
			//}
			//	Lprintf(4, "[INFO] DBMS live, but response is slow so not changed\n")
			//	return nil, DBCONN_DECLINE, cancel

		} else {
			Lprintf(1, "[ERR ] query error : %s\n", err)
			return nil, DBETC_ERR, cancel
		}
	}
	//defer cancel() // releases resources if slowOperation completes before timeout elapses

	return rows, 0, cancel
}

// 프로시저 응답코드 처리
func GetRespCode(rows *sql.Rows, procName string) int {
	var result int

	for rows.Next() {
		if err := rows.Scan(&result); err != nil {
			Lprintf(1, "[ERR ] %s first return scan error : %s\n", procName, err)
			result = 99
			return result
		}

		if result == 99 { // 프로시저 에러
			Lprintf(1, "[ERR ] dbms error %s : %d\n", procName, result)
			return result
		}

		if result == 1 { // 저장 조건에 충족하지 않아서 에러
			Lprintf(1, "[ERR ] %s is not satisfied with conditions : %d\n", procName, result)
			return result
		}

		if result == -1 { // 저장 조건에 충족하지 않아서 에러 (jdh)
			Lprintf(1, "[ERR ] %s is not satisfied with conditions : %d\n", procName, result)
			return result
		}
	}

	return result
}

// 프로시저 응답코드 처리 -> db sync가 필요한 경우 사용
func GetRespCodeSync(rows *sql.Rows, procName string) int {
	var result int

	for rows.Next() {
		if err := rows.Scan(&result); err != nil {
			Lprintf(1, "[ERR ] %s first return scan error : %s\n", procName, err)
			result = -1
			return result
		}

		if result < 0 { // 프로시저 에러
			Lprintf(1, "[ERR ] dbms error %s : %d\n", procName, result)
			return result
		} else if result > 0 {
			go NotiDbAgent(result)

		}
	}

	return result
}

func NotiDbAgent(reportIdx int) {
	sIdx := fmt.Sprintf("%d:salt", reportIdx)

	url := "http://" + Dbip[ConnIdx] + ":" + DBAgentPort + "/dbagent/notify?idx=" + EEncode([]byte(sIdx))
	req, err := http.NewRequest("GET", url, nil)
	Lprintf(4, "[INFO] noti request (%s)", url)

	req.Header.Set("Connection", "close")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		Lprintf(1, "[FAIL] noti request error (%s)", err)
		return
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		Lprintf(1, "[FAIL] noti response error (%s)", err)
		return
	}
	Lprintf(4, "[INFO] noti response (%s)", resp.Status)
	return
}

/*
 * 프로시저 응답코드 처리
 * 첫번째 리턴에서 응답코드 외 데이터가 존재할 경우 사용
 */
func GetRespCodeE(rows *sql.Rows, procName string) RespDbms {
	var resp = RespDbms{}

	for rows.Next() {
		if err := rows.Scan(&resp.Result, &resp.EventYn, &resp.RowCount); err != nil {
			Lprintf(1, "[ERR ] %s first return scan error : %s\n", procName, err)
			resp.Result = 99
			return resp
		}

		if resp.Result == 99 { // 프로시저 에러
			Lprintf(1, "[ERR ] dbms error %s : %d\n", procName, resp.Result)
			return resp
		}

		if resp.Result == 1 { // 저장 조건에 충족하지 않아서 에러
			Lprintf(1, "[ERR ] %s is not satisfied with conditions : %d\n", procName, resp.Result)
			return resp
		}
	}

	return resp
}

func Initdb(dbbd, dbid, dbps, dbip, dbpt, dbnm string) (*sql.DB, error) {
	Lprintf(4, "[INFO] new db connection try %s \n", dbip)

	var c *sql.DB
	var err error

	if dbbd == "mssql" {
		c, err = sql.Open("mssql", "server="+dbip+";user id="+dbid+";password="+dbps+";port="+dbpt+";database="+dbnm)
		if err != nil {
			Lprintf(1, "[FAIL] db connection error")
			return nil, err
		}
	} else {

		if Dbssl == 1 {

			/*
				ssl_ca (인증기관 인증서) : /smartagent/Plugins/DFA/ssl/ca-cert.pem
				ssl_cert (public key) : /smartagent/Plugins/DFA/ssl/ca-cert.pem
				ssl_key (private key) : /smartagent/Plugins/DFA/ssl/server-pkey.pem
			*/

			sslKeyPath := fmt.Sprintf("%s/ca-cert.pem", ConfDir)
			Lprintf(4, "[INFO] sslKeyPath (%s)\n", sslKeyPath)

			rootCertPool := x509.NewCertPool()
			pem, err := ioutil.ReadFile(sslKeyPath)
			if err != nil {
				Lprintf(1, "[FAIL] ssl error (%s)\n", err.Error())
			}

			if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
				Lprintf(1, "[FAIL] Failed to append PEM \n")
			}

			mysql.RegisterTLSConfig("custom", &tls.Config{
				RootCAs:            rootCertPool,
				InsecureSkipVerify: true,
			})

			c, err = sql.Open("mysql", dbid+":"+dbps+"@tcp("+dbip+":"+dbpt+")/"+dbnm+"?tls=custom")
			if err != nil {
				Lprintf(1, "[FAIL] db connection error")
				return nil, err
			}

		} else {
			c, err = sql.Open("mysql", dbid+":"+dbps+"@tcp("+dbip+":"+dbpt+")/"+dbnm)
			if err != nil {
				Lprintf(1, "[FAIL] db connection error")
				return nil, err
			}
		}

	}

	c.SetMaxIdleConns(10) // 재사용 connection 수
	c.SetMaxOpenConns(10) // 최대 connection 수

	// mysql client connection 유지 시간 default - 8 hour
	// connection 최대 유지 시간
	//c.SetConnMaxLifetime(time.Hour)

	return c, nil
}

// DBMS retry 기능이 들어간 쿼리(Select)
func QueryDBbyParam(query string, params ...interface{}) (*sql.Rows, error) {
	if DBc == nil {
		time.Sleep(10 * time.Millisecond)
	}

	DBMutex.RLock()
	rows, err := DBc.Query(query, params...)
	DBMutex.RUnlock()

	Lprintf(4, "[INFO] query resp string : %s\n", query)
	Lprintf(4, "[INFO] params resp string : %s\n", params)
	if err != nil {
		Lprintf(1, "[ERR ] Query error : %s \n", err)
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no route to host") {
			DbmsDuplexing()
		}
		// try one more
		DBMutex.RLock()
		defer DBMutex.RUnlock()
		return DBc.Query(query, params)
	}
	return rows, err
}

// DBMS retry 기능이 들어간 쿼리(Insert, Update, Delete)
func ExecDBbyParam(query string, params []interface{}) (int, error) {
	if DBc == nil {
		time.Sleep(10 * time.Millisecond)
	}

	DBMutex.RLock()
	ret, err := DBc.Exec(query, params...)
	DBMutex.RUnlock()

	Lprintf(4, "[INFO] query resp string : %s\n", query)
	Lprintf(4, "[INFO] params resp string : %s\n", params)
	if err != nil {
		Lprintf(1, "[ERR ] Query error : %s \n", err)
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no route to host") {
			DbmsDuplexing()

			// try one more
			DBMutex.RLock()
			ret, err = DBc.Exec(query, params...)
			DBMutex.RUnlock()

			if err != nil {
				Lprintf(1, "[ERR ] Query error : %s \n", err)
				return 0, err
			}

			n, err := ret.RowsAffected()
			Lprintf(4, "[INFO] %d row affected \n", n)
			return int(n), err
		}
		return 0, err
	}

	// sql.Result.RowsAffected() 체크
	n, err := ret.RowsAffected()
	Lprintf(4, "[INFO] %d row affected \n", n)
	return int(n), err
}


// Echo 미사용 select 쿼리시 사용
func SelectData(sqlQuery string, params map[string]string) ([]map[string]string, error) {

	//params := GetParamJsonMap(c)
	var errMap []map[string]string
	// 파라메터 맵으로 쿼리 변환
	selectQuery, err := SetUpdateParam(sqlQuery, params)
	if err != nil {
		Lprintf(4, "[INFO] %d call GetQueryJson \n", err)
		return errMap, err
	}
	// 쿼리 실행 후 JSON 형태로 결과 받기

	resultData, err := QueryMap(selectQuery)
	if err != nil {
		Lprintf(4, "[INFO] %d call QueryMap \n", err)
		return errMap, err
	}

	return resultData, nil
}


// sEcho 미사용 쿼리시 사용 --select 쿼리를 날려서  Map으로 만들어서 리턴하는  함수
func QueryMap(query string) ([]map[string]string, error) {


	Lprintf(4, "[INFO] call Query (%s) \n", query)
	DBMutex.RLock()
	rows, dberr := DBc.Query(query)
	DBMutex.RUnlock()
	defer rows.Close()
	if dberr != nil {
		Lprintf(4, "[INFO](%s) call error : %s\n", query, dberr)
		return nil, dberr
	}
	var resultMap []map[string]string

	// 응답코드
	cols, _ := rows.Columns()
	for i := 0; rows.Next(); i++ {
		resultOne := make(map[string]string)

		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		for i, _ := range cols {
			val := columnPointers[i].(*interface{})
			value := fmt.Sprintf("%s", *val)
			resultOne[cols[i]] = value
		}
		resultMap = append(resultMap, resultOne)
	}

	return resultMap, nil
}