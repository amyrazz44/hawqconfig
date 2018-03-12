package server

import (
	"hawqconfig/common"
	"log"
	"io/ioutil"
	"encoding/json"
)

// baseParamFilePath contains the system parameters and gucs which can be edit.
//var baseParamFilePath string = "/Users/abai/workspace/Go/src/hawqconfig/server/baseParam.json"
var baseParamFilePath string = "/tmp/baseParam.json"


type SysParamPrefix struct {
	HdfsUrlPrefix        string `json:"hdfs_url_prefix"`
	EtcdServerIP         string `json:"etcd_server_ip"`
	EtcdServerDirPrefix  string `json:"etcd_server_dir_prefix"`
	MasterStorageType    string `json:"master_storage_type"`
	MasterMountDirPrefix string `json:"master_mount_dir_prefix"`
}

var sysParam = map[string]string{
	//HAWQ system guc keys
	"EtcdServerIP":  "hawq_rm_etcd_server_ip",
	"EtcdEnable":    "hawq_rm_etcd_enable",
	"EtcdServerDir": "hawq_rm_etcd_server_dir",
	"HdfsUrl":       "hawq_dfs_url",
	//HAWQ system parameters
	"MasterStorageType": "master_storage_type",
	"MasterMountDir":    "master_mount_dir",
}

// getParamPrefix gets system related parameter by reading the paramFile.
func getParamPrefix() SysParamPrefix {
	raw, err := ioutil.ReadFile(baseParamFilePath)
	if err != nil {
		log.Fatal(err.Error())
	}
	var param SysParamPrefix
	json.Unmarshal(raw, &param)
	return param
}

// isSysParam judges the guc is a system parameter or not.
func isSysParam(configMap map[string]string) bool {
	for key := range (configMap) {
		for _, sysValue := range (sysParam) {
			if key == sysValue {
				return true
			}
		}
	}
	return false
}

// generateHAWQConfigInfo generates hawq config info and return.
func generateHAWQConfigInfo(clusterName string) *common.ConfigService {
	cs := new(common.ConfigService)
	cs.ClusterName = clusterName
	cs.SysGucs = generateHAWQSysGucs(clusterName)
	cs.UserGucs = generateHAWQUserGucs()
	cs.SysParams = generateHAWQSysParams(clusterName)
	return cs
}

// updateHAWQConfigInfo updates hawq gucs which user want to modify.
func updateHAWQConfigInfo(hcInfo *common.ConfigService, configMap map[string]string) *common.ConfigService {
	for key, value := range (configMap) {
		hcInfo.UserGucs[key] = value
	}
	return hcInfo
}

// generateHAWQSysGucs generates hawq system gucs.
func generateHAWQSysGucs(clusterName string) map[string]interface{} {
	param := getParamPrefix()
	sysGucs := make(map[string]interface{})
	sysGucs[sysParam["EtcdServerIP"]] = param.EtcdServerIP
	sysGucs[sysParam["EtcdEnable"]] = "true"
	sysGucs[sysParam["EtcdServerDir"]] = param.EtcdServerDirPrefix
	//TODO: HdfsBaseUrl + clusterName; Check hdfs url empty
	sysGucs[sysParam["HdfsUrl"]] = generateHdfsUrl(param.HdfsUrlPrefix, clusterName)
	return sysGucs
}

// generateHAWQUserGucs generates hawq user gucs.
func generateHAWQUserGucs() map[string]interface{} {
	userGucs := make(map[string]interface{})
	userGucs["testUserGuc"] = "TODO"
	return userGucs
}

// generateHAWQSysParams generates hawq system parameters.
func generateHAWQSysParams(clusterName string) map[string]string {
	param := getParamPrefix()
	sysParams := make(map[string]string)
	sysParams[sysParam["MasterStorageType"]] = param.MasterStorageType
	//TODO: check mount dir empty
	sysParams[sysParam["MasterMountDir"]] = param.MasterMountDirPrefix + clusterName
	return sysParams
}

// generateHdfsUrl generates hawq hdfs url.
func generateHdfsUrl(hdfsBaseUrl string, clusterName string) string {
	hdfsUrl := hdfsBaseUrl + clusterName
	//checkHdfsUrlExists()
	log.Print("hdfs url is : ", hdfsUrl)
	return hdfsUrl
}
