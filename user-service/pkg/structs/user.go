package structs

import "time"

type UserInfo struct {
	Id          string    `json:"Id"`
	User_id     string    `json:"User_id"`
	Barber_id   string    `json:"Barber_id"`
	Start_time  time.Time `json:"Start_time"`
	Finish_time time.Time `json:"Finish_time"`
	Date        string    `json:"Date"`
	Created_at  time.Time `json:"Created_at"`
	Updated_at  time.Time `json:"Updated_at"`
	Deleted_at  time.Time `json:"Delted_at"`
}
