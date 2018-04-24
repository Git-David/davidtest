package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"

	uuid "github.com/satori/go.uuid"

	_ "github.com/lib/pq"
)

func main() {

	var re = StartTrainModel()
	print(re)

}

func StartTrainModel() bool {
	const (
		host     = "localhost"
		port     = 5433
		user     = "davidliu"
		password = ""
		dbname   = "davidliu"
	)

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	var re = GetDBTableInfoForTraining(db, "pricepaid_train")

	temp_namedpipe := "/tmp/temp_namedpipe_" + uuid.Must(uuid.NewV4()).String()

	syscall.Mkfifo(temp_namedpipe, 0600)

	cmd := exec.Command("python3", "train.py", temp_namedpipe)
	// cmd.Start()

	err = cmd.Start()
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	// to open pipe to write
	pipefile, _ := os.OpenFile(temp_namedpipe, os.O_WRONLY|os.O_SYNC, 0600)

	pipefile.WriteString(re)
	pipefile.Close()

	return true

}

func GetDBTableInfoForTraining(db *sql.DB, table_name string) string {
	features := []int{2, 3, 4, 5, 6, 7, 8, 9, 10}
	// features := []int{2}

	labels := []int{1}

	type ColInfo struct {
		index    int
		col_name string
		col_type string
	}

	type TableInfo struct {
		Features    []int           `json:"features"`
		Labels      []int           `json:"labels"`
		Col_types   []string        `json:"col_types"`
		Onehot_cols map[int]int     `json:"onehot_cols"`
		Avg         map[int]float64 `json:"avg"`
		VarPop      map[int]float64 `json:"var_pop"`
	}

	var table_info = TableInfo{}
	table_info.Labels = labels
	table_info.Features = features

	//a list holds all the "string" types for one-hot.
	all_string_types := []string{}
	all_string_types = append(all_string_types, "text")
	all_string_types = append(all_string_types, "char")
	all_string_types = append(all_string_types, "varchar")

	//a list holds all the types of numbers, could be calculated for mean and var.
	all_number_types := []string{}
	all_number_types = append(all_number_types, "smallint")
	all_number_types = append(all_number_types, "int")
	all_number_types = append(all_number_types, "integer")
	all_number_types = append(all_number_types, "bigint")
	all_number_types = append(all_number_types, "real")
	all_number_types = append(all_number_types, "double precision")
	all_number_types = append(all_number_types, "numeric")
	//date and time are also considered as numbers as they stored as seconds in db.
	all_number_types = append(all_number_types, "date")
	all_number_types = append(all_number_types, "time")

	defer db.Close()

	//remove this line when OUSHUDB new executor supports DISTINCT.
	sqlStatement := "set new_executor = off"
	_, err := db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf("select column_name, data_type from information_schema.columns  where table_name = '%s' order by ordinal_position", table_name)
	rows, err := db.Query(sqlStatement)
	if err != nil {
		log.Fatal(err)
	}

	col_types := []string{}
	col_names := []string{}

	var col_infos []ColInfo

	i := 0
	for rows.Next() {
		var col_type string
		var col_name string
		var col_info = ColInfo{}

		rows.Scan(&col_name, &col_type)
		var origin_type = col_type
		if strings.Contains(col_type, "time") {
			col_type = "long"
		}
		if strings.Contains(col_type, "date") {
			col_type = "integer"
		}

		col_info.col_name = col_name
		col_info.col_type = origin_type
		col_info.index = i
		i = i + 1

		col_infos = append(col_infos, col_info)
		col_types = append(col_types, col_type)
		col_names = append(col_names, col_name)
	}

	//type mapping for tensorflow
	postgres_tensorflow_typemapping := make(map[string]string)
	postgres_tensorflow_typemapping["text"] = "tf.string"
	postgres_tensorflow_typemapping["char"] = "tf.string"
	postgres_tensorflow_typemapping["varchar"] = "tf.string"

	postgres_tensorflow_typemapping["smallint"] = "tf.int64"
	postgres_tensorflow_typemapping["int"] = "tf.int64"
	postgres_tensorflow_typemapping["integer"] = "tf.int64"
	postgres_tensorflow_typemapping["bigint"] = "tf.int64"
	postgres_tensorflow_typemapping["long"] = "tf.int64"

	postgres_tensorflow_typemapping["real"] = "tf.float32"
	postgres_tensorflow_typemapping["double precision"] = "tf.float32"
	postgres_tensorflow_typemapping["numeric"] = "tf.float32"

	tensorflow_types := []string{}
	for _, col_type := range col_types {
		if value, ok := postgres_tensorflow_typemapping[col_type]; ok {
			tensorflow_types = append(tensorflow_types, value)
		} else {
			fmt.Println("key not found in postgres_tensorflow_typemapping")
		}

	}

	table_info.Col_types = tensorflow_types
	var one_hots = make(map[int]int)

	for _, row := range col_infos {

		// filt column sin features array
		if !IntInSlice(row.index, features) {
			continue
		}

		//filt columns that are of string type, thus could be used as one hot.
		if !stringContainsOneOfSlice(row.col_type, all_string_types) {
			continue
		}

		sqlStatement = fmt.Sprintf("select count(DISTINCT %s) from %s", row.col_name, table_name)
		rows, err = db.Query(sqlStatement)
		if err != nil {
			log.Fatal(err)
		}

		// value_array := []string{}
		var count_value int

		for rows.Next() {
			rows.Scan(&count_value)
			// value_array = append(value_array, str_value)
		}

		one_hots[row.index] = count_value

		// if len(value_array) > 1 {
		// 	one_hots[row.index] = value_array
		// }
		table_info.Onehot_cols = one_hots
	}

	//avg and var_pop

	var avgs = make(map[int]float64)
	var var_pops = make(map[int]float64)

	for _, row := range col_infos {

		// filt column sin features array
		if !IntInSlice(row.index, features) {
			continue
		}

		//filt columns that are of numbver type, thus could be calcuated for avg and var_pop.
		if !stringContainsOneOfSlice(row.col_type, all_number_types) {
			continue
		}

		if strings.Contains(row.col_type, "time") || strings.Contains(row.col_type, "date") ||
			strings.Contains(row.col_type, "interval") {
			// slect avg(extract(epoch from dateoftransfer AT TIME ZONE 'UTC')) from pricepaid_orc
			sqlStatement = fmt.Sprintf("select avg(extract(epoch from \"%s\" AT TIME ZONE 'UTC')), var_pop(extract(epoch from \"%s\"AT TIME ZONE 'UTC')) from %s ", row.col_name, row.col_name, table_name)

		} else {
			sqlStatement = fmt.Sprintf("select avg(\"%s\"), var_pop(\"%s\") from %s ", row.col_name, row.col_name, table_name)

		}

		rows, err = db.Query(sqlStatement)
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var avg_value float64
			var var_value float64

			rows.Scan(&avg_value, &var_value)

			avgs[row.index] = avg_value
			var_pops[row.index] = var_value

		}

		table_info.Avg = avgs
		table_info.VarPop = var_pops
	}

	result_json, err := json.Marshal(table_info)
	return string(result_json)
}

func stringContainsOneOfSlice(str string, list []string) bool {
	for _, v := range list {
		if strings.Contains(str, v) {
			return true
		}
	}
	return false
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func IntInSlice(a int, list []int) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
