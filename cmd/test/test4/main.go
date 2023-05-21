package main

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/mitchellh/mapstructure"
)

const timeFormart = "2006-01-02 15:04:05"

// 对struct增加 MarshalJSON ,UnmarshalJSON , String 方法，实现自定义json输出格式与打印方式
type BsonTime struct {
	time.Time
}

func (t BsonTime) String() string {
	return t.Time.Format(timeFormart)
}

// 实现它的json序列化方法
func (t BsonTime) MarshalJSON() ([]byte, error) {
	var stamp = fmt.Sprintf("\"%s\"", t.Time.Format("2006-01-02 15:04:05"))
	return []byte(stamp), nil
}

func (t *BsonTime) UnmarshalJSON(data []byte) error {
	if err := t.Time.UnmarshalJSON(data); err != nil {
		return err
	}
	return nil
}

func (t BsonTime) GetBSON() (interface{}, error) {
	if t.IsZero() {
		return nil, nil
	}
	return t.Time, nil
}

func (t *BsonTime) SetBSON(raw bson.Raw) error {
	var tm time.Time
	if err := raw.Unmarshal(&tm); err != nil {
		return err
	}
	t.Time = tm
	return nil
}

func ToTimeHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if !reflect.DeepEqual(t, reflect.TypeOf(BsonTime{})) {
			return data, nil
		}
		var tTime time.Time
		var err error
		switch f.Kind() {
		case reflect.String:
			tTime, err = time.Parse("2006-01-02 15:04:05", data.(string))
		case reflect.Float64:
			tTime, err = time.Unix(0, int64(data.(float64))*int64(time.Millisecond)), nil
		case reflect.Int64:
			tTime, err = time.Unix(0, data.(int64)*int64(time.Millisecond)), nil
		default:
			return data, nil
		}
		if err != nil {
			return nil, err
		}
		return BsonTime{
			Time: tTime,
		}, nil
	}
}

// Map2StructWithBsonTime user `mapstructure` convert map to struct, with process time to BsonTime.
func Map2StructWithBsonTime(input map[string]interface{}, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata:   nil,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(ToTimeHookFunc()),
		Result:     result,
		TagName:    "json",
	})
	if err != nil {
		return err
	}
	if err := decoder.Decode(input); err != nil {
		return err
	}
	return nil
}

var rawStr = `
{
    "created_by": "7290310f-3574-4520-9ef1-79188bc7c997",
    "created_time": 1563884700000,
    "last_edited_time": 1563884700000,
    "last_edited_by": "7290310f-3574-4520-9ef1-79188bc7c997",
    "last_reply_at": "2019-07-01 12:34:56"
}
`

// AuthorTime must define `JSON TAG`.
type AuthorTime struct {
	CreatedBy   string   `json:"created_by"`
	CreatedAt   BsonTime `json:"created_time"`
	UpdatedBy   string   `json:"last_edited_by"`
	UpdatedAt   BsonTime `json:"last_edited_time"`
	LastReplyAt BsonTime `json:"last_reply_at"`
}

func main() {
	var paramsBind map[string]interface{}              // 模拟gin的参数绑定
	err := json.Unmarshal([]byte(rawStr), &paramsBind) //paramsBind需要是引用类型
	if err != nil {
		panic(err)
	}

	// 将参数转化为struct
	var author AuthorTime
	err = Map2StructWithBsonTime(paramsBind, &author)
	if err != nil {
		panic(err)
	}

	// 转为json
	toJson, err := json.Marshal(&author)
	if err != nil {
		panic(err)
	}
	log.Println(string(toJson))
}
