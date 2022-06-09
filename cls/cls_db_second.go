package cls

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

var Dbbd2 string   // dbms name - mssql, mysql
var Dbid2 string   // dbms id
var Dbps2 string   // dbms password
var Dbpt2 string   // dbms port
var Dbnm2 string   // dbms instance
var Dbssl2 int     // dbms ssl encrypt option
var DbTimeOut2 int // dbms timeout

var DBc2 *sql.DB
var DBRand2 *rand.Rand
var ConnIdx2 int     // 현재 접속중인 ip 배열 index - 최초는 0번째 ip로 연결
var ConnLen2 int     // 접속할 ip 갯수
var Dbip2 []string   // dbms ip
var DbipList2 string // dbms ip list

var DBMutex2 sync.RWMutex

func Db_conf2(fname string) int {
	Lprintf(4, "[INFO] conf start (%s)\n", fname)

	// dbms connect address
	/*
		dbip, r := GetTokenValue("HOST_DBMS", fname)
		if r == CONF_ERR {
			Lprintf(1, "[FAIL] DBMS not exist value\n")
			return (-1)
		}
	*/

	DBMutex2.Lock()
	defer DBMutex2.Unlock()

	dbip2, r := GetTokenValue("HOST_DBMS2", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS not exist value\n")
		return (-1)
	}
	DbipList2 = dbip2
	Dbip2 = strings.Split(dbip2, "&")
	ConnLen2 = len(Dbip2)

	s1 := rand.NewSource(time.Now().UnixNano())
	DBRand2 = rand.New(s1)
	ConnIdx2 = DBRand2.Intn(ConnLen2)

	// dbms connect id
	dbid, r := GetTokenValue("ID_DBMS2", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_ID not exist value\n")
		return (-1)
	}
	Dbid2 = dbid
	Lprintf(4, "[INFO] Dbid : (%s)\n", Dbid2)

	// dbms connect password
	dbps, r := GetTokenValue("PASSWD_DBMS2", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_PASS not exist value\n")
		return (-1)
	}
	Dbps2 = dbps
	Lprintf(4, "[INFO] Dbps : (%s)\n", Dbps2)

	// dbms instance name
	dbnm, r := GetTokenValue("NAME_DBMS2", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_DB not exist value\n")
		return (-1)
	}
	Dbnm2 = dbnm
	Lprintf(4, "[INFO] Dbnm : (%s)\n", Dbnm2)

	// dbms instance name
	dbpt, r := GetTokenValue("PORT_DBMS2", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_DB not exist value\n")
		return (-1)
	}
	Dbpt2 = dbpt
	Lprintf(4, "[INFO] Dbpt : (%s)\n", Dbpt2)

	// dbms brand
	dbbd, r := GetTokenValue("DBMS2", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] DBMS_DB not exist value\n")
		dbbd = "mysql"
	}
	Dbbd2 = dbbd
	Lprintf(4, "[INFO] Dbbd : (%s)\n", Dbbd2)

	dbssl, r := GetTokenValue("SSL_DBMS2", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] SSL_DBMS not exist value\n")
	}

	if dbssl == "Y" {
		Dbssl2 = 1
		Lprintf(4, "[INFO] DBMS SSL ENCRYPT USE \n")
	}

	// dbms timeout
	dbTimeout, r := GetTokenValue("TIMEOUT_DBMS2", fname)
	if r == CONF_ERR {
		Lprintf(1, "[FAIL] TIMEOUT_DBMS not exist value, so set 10 seconds\n")
		dbTimeout = "10" // db query timeout config에 미설정시 default 값으로 10초 설정
		//return (-1)
	}
	DbTimeOut2, _ = strconv.Atoi(dbTimeout)
	Lprintf(4, "[INFO] DbTimeOut : %d\n", DbTimeOut2)

	DBc2, _ = Initdb(dbbd, dbid, dbps, Dbip2[ConnIdx2], dbpt, dbnm)

	if DBc2 == nil {
		Lprintf(4, "[INFO] Db connect error \n")
		return (-1)
	}

	go CheckDbmsLive2()

	return 0
}

// config에 등록된 다른 DBMS로 연결한다.
func DbmsDuplexing2() {
	Lprintf(4, "[INFO] DBMS connection change now (%s) ---> next \n", Dbip2[ConnIdx2])
	if err := DBc2.Close(); err != nil { // 기존 DB CLOSE
		Lprintf(1, "[ERR ] DBc close is error : [%s]\n", err)
	}

	newIdx := (ConnIdx2 + 1) % ConnLen2
	c, err := Initdb(Dbbd2, Dbid2, Dbps2, Dbip2[newIdx], Dbpt2, Dbnm2)
	DBc2 = c
	if err == nil {
		ConnIdx2 = newIdx
	}
	return
}

// DBMS 헬스체크 - 10 초에 한번 - auto scal out 없음
func CheckDbmsLive2() {
	Lprintf(4, "[INFO] check db connection start \n")
	for {
		time.Sleep(10 * time.Second)
		err := DBc2.Ping()

		if err != nil {
			DBMutex2.Lock()
			DbmsDuplexing2()
			DBMutex2.Unlock()
		}
	}
}

// DBMS retry 기능이 들어간 쿼리
func QueryDB2(query string) (*sql.Rows, error) {
	if DBc2 == nil {
		time.Sleep(10 * time.Millisecond)
	}

	DBMutex2.RLock()
	rows, err := DBc2.Query(query)
	DBMutex2.RUnlock()

	//Lprintf(4, "[INFO] query resp string : %s\n", err)
	if err != nil {
		Lprintf(1, "[ERR ] Query error : %s \n", err)
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no route to host") {
			DbmsDuplexing2()
		}
		// try one more
		DBMutex2.RLock()
		defer DBMutex2.RUnlock()
		return DBc2.Query(query)
	}
	return rows, err
}

// 일반적인 SELECT 쿼리 시 사용 -결과 string
func GetSelectData2(sqlQuery string, params map[string]string) ([]map[string]string, error) {

	//params := GetParamJsonMap(c)
	var errMap []map[string]string
	// 파라메터 맵으로 쿼리 변환
	selectQuery, err := SetUpdateParam(sqlQuery, params)
	if err != nil {
		Lprintf(4, "[INFO] %d call GetQueryJson \n", err)
		return errMap, err
	}
	// 쿼리 실행 후 JSON 형태로 결과 받기

	Lprintf(4, "[INFO] selectQuery(%s)\n", selectQuery)

	resultData, err := QueryMapColumn2(selectQuery)
	if err != nil {
		Lprintf(4, "[INFO] %d call QueryMapColumn \n", err)
		return errMap, err
	}

	return resultData, nil
}

// select 쿼리를 날려서  Map으로 만들어서 리턴하는  함수
func QueryMapColumn2(query string) ([]map[string]string, error) {

	DBMutex2.RLock()
	rows, dberr := DBc2.Query(query)
	DBMutex2.RUnlock()
	defer rows.Close()
	if dberr != nil {
		Lprint(1, "[ERROR] db err(%s)\n", dberr.Error())
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

// DBMS retry 기능이 들어간 쿼리(Insert, Update, Delete)
func ExecDBbyParam2(query string, params []interface{}) (int, error) {
	if DBc2 == nil {
		time.Sleep(10 * time.Millisecond)
	}

	DBMutex2.RLock()
	ret, err := DBc2.Exec(query, params...)
	DBMutex2.RUnlock()

	Lprintf(4, "[INFO] query resp string : %s\n", query)
	Lprintf(4, "[INFO] params resp string : %s\n", params)
	if err != nil {
		Lprintf(1, "[ERR ] Query error : %s \n", err)
		if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no route to host") {
			DbmsDuplexing()

			// try one more
			DBMutex2.RLock()
			ret, err = DBc2.Exec(query, params...)
			DBMutex2.RUnlock()

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