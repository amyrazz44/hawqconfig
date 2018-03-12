package server

import (
	"time"
	"log"
	"encoding/json"
	"context"
	"github.com/coreos/etcd/client"
	"hawqconfig/common"
)

// getEtcdServerUrl gets etcd server url from config file.
func getEtcdServerUrl() (serverIp, serverDir string) {
	param := getParamPrefix()
	return param.EtcdServerIP, param.EtcdServerDirPrefix
}

// connectToEtcd connects to etcd server.
func connectToEtcd() client.KeysAPI {
	var etcdServer []string
	etcdServerUrl, _ := getEtcdServerUrl()
	etcdServer = append(etcdServer, etcdServerUrl)
	cfg := client.Config{
		Endpoints: etcdServer,
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	kapi := client.NewKeysAPI(c)
	return kapi
}

// storeToEtcd stores the hawq config info into etcd by given key(cluster name) and value.
func storeToEtcd(key string, value *common.ConfigService) {
	_, etcdServerDir := getEtcdServerUrl()
	key = etcdServerDir + key
	log.Print("Store to etcd server")
	log.Print("key is : ", key)
	log.Print("value is: ", value)
	kapi := connectToEtcd()
	jsonStr, err := json.Marshal(value)
	if err != nil {
		log.Fatal("Format ConfigService to json failed")
	} else {
		log.Print("jsonStr in storeToEtcd is: ", string(jsonStr))
	}
	//Set value
	resp, err := kapi.Set(context.Background(), key, string(jsonStr), nil)
	if err != nil {
		log.Fatal(err)
	} else {
		// print common key info
		log.Printf("Set is done. Metadata is %q\n", resp)
	}
	log.Print("Getting key value")
	resp, err = kapi.Get(context.Background(), key, nil)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Get is done. Metadata is %q\n", resp)
		log.Printf("%q key has %q value\n", resp.Node.Key, resp.Node.Value)
	}
}

// getConfigFromEtcd gets hawq config info from etcd by given key(cluster name).
func getConfigFromEtcd(key string) *common.ConfigService {
	_, etcdServerDir := getEtcdServerUrl()
	key = etcdServerDir + key
	log.Print("Get value from etcd server")
	log.Print("key is: ", key)
	kapi := connectToEtcd()
	log.Print("Getting key value")
	resp, err := kapi.Get(context.Background(), key, nil)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Get is done. Metadata is %q\n", resp)
		log.Printf("%q key has %q value\n", resp.Node.Key, resp.Node.Value)
	}
	value := &common.ConfigService{}
	err = json.Unmarshal([]byte(resp.Node.Value), &value)
	if err != nil {
		log.Fatal("Unmarshal failed!")
	} else {
		log.Print("Unmarshal success")
	}
	return value
}

// delFromEtcd deletes hawq config info from etcd by given key(cluster name).
func delFromEtcd(key string) bool {
	_, etcdServerDir := getEtcdServerUrl()
	key = etcdServerDir + key
	kapi := connectToEtcd()
	resp, err := kapi.Delete(context.Background(), key, nil)
	if err != nil {
		log.Fatal(err)
		return false
	} else {
		log.Printf("Delete is done. Metadata is %q\n", resp)
	}
	return true
}

// checkEtcdKeyExists checks etcd key exists or not.
func checkEtcdKeyExists(key string) bool {
	kapi := connectToEtcd()
	_, etcdServerDir := getEtcdServerUrl()
	key = etcdServerDir + key
	log.Print("key in checkEtcdKeyExists is : ", key)
	_, err := kapi.Get(context.Background(), key, nil)
	flag := false
	if err == nil {
		flag = true
	}
	log.Print("flag in checkEtcdKeyExists is : ", flag)
	return flag
}
