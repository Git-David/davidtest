package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

func main() {

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
	var re = GetDBTableInfoForTraining(db, "pricepaid_orc")
	fmt.Println(re)

}

func GetDBTableInfoForTraining(db *sql.DB, table_name string) string {
	features := []int{2, 3, 4, 5, 6, 7, 8, 9, 10}
	labels := []int{1}

	type OneHot struct {
		Index    int
		Elements []string
	}

	type ColInfo struct {
		index    int
		col_name string
		col_type string
	}

	type TableInfo struct {
		Features    []int            `json:"features"`
		Labels      []int            `json:"labels"`
		Col_types   []string         `json:"col_types"`
		Onehot_cols map[int][]string `json:"onehot_cols"`
	}

	var table_info = TableInfo{}
	table_info.Labels = labels
	table_info.Features = features

	//a list holds all the types for one-hot.
	all_string_types := []string{}
	all_string_types = append(all_string_types, "text")
	all_string_types = append(all_string_types, "char")

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
		if strings.Contains(col_type, "timestamp") {
			col_type = "long"
		}
		if strings.Contains(col_type, "date") {
			col_type = "integer"
		}

		col_info.col_name = col_name
		col_info.col_type = col_type
		col_info.index = i
		i = i + 1

		col_infos = append(col_infos, col_info)
		col_types = append(col_types, col_type)
		col_names = append(col_names, col_name)
	}

	table_info.Col_types = col_types
	var one_hots = make(map[int][]string)

	for _, row := range col_infos {

		// filt column sin features array
		if !IntInSlice(row.index, features) {
			continue
		}

		//filt columns that are of string type, thus could be used as one hot.
		if !stringContainsOneOfSlice(row.col_type, all_string_types) {
			continue
		}

		sqlStatement = fmt.Sprintf("select DISTINCT %s from %s order by %s", row.col_name, table_name, row.col_name)
		rows, err = db.Query(sqlStatement)
		if err != nil {
			log.Fatal(err)
		}

		value_array := []string{}

		for rows.Next() {
			var str_value string
			rows.Scan(&str_value)
			value_array = append(value_array, str_value)
		}

		one_hots[row.index] = value_array
		table_info.Onehot_cols = one_hots
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
