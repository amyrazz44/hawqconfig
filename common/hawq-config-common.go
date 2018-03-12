package common

import (
	_ "fmt"
)

// ConfigService holds the hawq config info.
type ConfigService struct {
	ClusterName string
	SysGucs     map[string]interface{} //System gucs which couldn't be set by user : e.g. hdfsUrl, etcd related gucs.
	UserGucs    map[string]interface{} //User gucs which could be set by user.
	SysParams   map[string]string      //System parameters which couldn't be set by user:
	// e.g. hawq master storage type , hawq master mount directory.
}
