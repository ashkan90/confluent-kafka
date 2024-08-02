package confluent_kafka

import "reflect"

func SafeStringExport(v *string) string {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		return val.Elem().Interface().(string)
	}

	return ""
}
