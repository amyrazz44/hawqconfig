package main

import (
	_"testing"
	"hawqconfig/client"
	"log"
)

func main() {
	clusterName := "6"
	client.CreateConfig(clusterName)
	log.Print("Done client.CreateConfig")
	client.GetConfig(clusterName)
	log.Print("Done client.GetConfig")
	configMap := make(map[string]string)
	configMap["hawq_master_address_host"] = "aaaaaa"
	configMap["hawq_standby_address_host"] = "127.0.0.1"
	client.UpdateConfig(clusterName, configMap)
	log.Print("Done client.UpdateConfig")
	client.DeleteConfig(clusterName)
	log.Print("Done client.DeleteConfig")
}
