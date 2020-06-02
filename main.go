package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
)

type Statics struct {
	LastStatsSync   string `json:"last_stats_sync"`
	LastStatsUpdate string `json:"last_stats_update"`
	Stats           struct {
		TotalBytes        int64 `json:"total_bytes"`
		TotalBytesRounded int64 `json:"total_bytes_rounded"`
		TotalEntries      int64 `json:"total_entries"`
	} `json:"stats"`
}

type Quota struct {
	Auid        int64 `json:"auid"`
	BucketQuota struct {
		Enabled    bool  `json:"enabled"`
		MaxObjects int64 `json:"max_objects"`
		MaxSizeKb  int64 `json:"max_size_kb"`
	} `json:"bucket_quota"`
	Caps             []interface{} `json:"caps"`
	DefaultPlacement string        `json:"default_placement"`
	DisplayName      string        `json:"display_name"`
	Email            string        `json:"email"`
	Keys             []struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
		User      string `json:"user"`
	} `json:"keys"`
	MaxBuckets    int64         `json:"max_buckets"`
	OpMask        string        `json:"op_mask"`
	PlacementTags []interface{} `json:"placement_tags"`
	Subusers      []interface{} `json:"subusers"`
	Suspended     int64         `json:"suspended"`
	SwiftKeys     []interface{} `json:"swift_keys"`
	TempURLKeys   []interface{} `json:"temp_url_keys"`
	UserID        string        `json:"user_id"`
	UserQuota     struct {
		Enabled    bool  `json:"enabled"`
		MaxObjects int64 `json:"max_objects"`
		MaxSizeKb  int64 `json:"max_size_kb"`
	} `json:"user_quota"`
}


func main() {
	var userslist[] string
	var statics Statics
	var quota Quota
	f, err3 := os.Create("staics.xls")
	if err3 != nil{
		fmt.Println(err3)
	}
	defer f.Close()
	f.WriteString("\xEF\xBB\xBF")
	w := csv.NewWriter(f)
	w.Write([]string{"ID","Totalsize","objnum", "bucket_maxkb", "bucket_maxobj","user_maxkb", "user_maxobj", "bucket_quota_enable", "user_quota_enable"})

	output,err := exec.Command("radosgw-admin","metadata","list","user").CombinedOutput()
	if err != nil {
		fmt.Printf("0")
		log.Fatal(err)
	}else {
		err1:= json.Unmarshal([]byte(output), &userslist)
		if err1 != nil {
			fmt.Println(err1)
		}
	}
	for i := 0; i<len(userslist);i++{
		bucket_output,err2 := exec.Command("radosgw-admin","user","stats","--uid",userslist[i]).CombinedOutput()
		quota_output,_ := exec.Command("radosgw-admin","user","info","--uid",userslist[i]).CombinedOutput()
		if err2 != nil {
			fmt.Println(err2)
			continue
		}else {
			json.Unmarshal([]byte(bucket_output), &statics)
			json.Unmarshal([]byte(quota_output), &quota)

			}
		w.Write([]string{userslist[i],
			strconv.FormatInt(statics.Stats.TotalBytesRounded ,10),
			strconv.FormatInt(statics.Stats.TotalEntries,10),
			strconv.FormatInt(quota.BucketQuota.MaxSizeKb,10),
			strconv.FormatInt(quota.BucketQuota.MaxObjects,10),
			strconv.FormatInt(quota.UserQuota.MaxSizeKb,10),
			strconv.FormatInt(quota.UserQuota.MaxObjects,10),
			strconv.FormatBool(quota.BucketQuota.Enabled),
			strconv.FormatBool(quota.UserQuota.Enabled)})
	}
	w.Flush()
}
