package client

import (
	"github.com/emicklei/go-restful"
	"log"
	"net/http"
	"strings"
	"hawqconfig/common"
	"hawqconfig/server"
	"encoding/json"
	"bytes"
)

var ServerURL string = "http://localhost:8090"

// CreateConfig creates the hawq config info by given cluster name.
func CreateConfig(clusterName string) *common.ConfigService {
	req, err := http.NewRequest("PUT", ServerURL+"/hawqconfig/"+clusterName, strings.NewReader(clusterName))
	req.Header.Set("Content-Type", restful.MIME_JSON)
	log.Print("Request in CreateConfig is : ", req)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Print("Unexpected error in sending req: %s", err)
	}
	if resp.StatusCode != http.StatusCreated {
		log.Print("Unexpected response: %s, expected: %s", resp.StatusCode, http.StatusOK)
	}
	return convertRespToConfig(resp)
}

// GetConfig gets the hawq config info by given cluster name.
func GetConfig(clusterName string) *common.ConfigService {
	//TODO: try several times
	resp, err := http.Get(ServerURL + "/hawqconfig/" + clusterName)
	if err != nil {
		log.Fatal("unexpected error in GET /hawqconfig/1: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		log.Print("unexpected response: %s, expected: %s", resp.StatusCode, http.StatusOK)
	}
	return convertRespToConfig(resp)
}

// UpdateConfig updates the hawq config info by given cluster name and the user gucs to change.
// TODO:map[string]interface{} ??
func UpdateConfig(clusterName string, configMap map[string]string) *common.ConfigService {
	jsonStr, err := json.Marshal(configMap)
	if err != nil {
		log.Print("json marshal failed in UpdataConfig:", err)
	}
	req, err := http.NewRequest("POST", ServerURL+"/hawqconfig/"+clusterName, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", restful.MIME_JSON)
	log.Print("Request in UpdateConfig is : ", req)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Print("Unexpected error in sending req: %s", err)
	}

	return convertRespToConfig(resp)
}

// DeleteConfig deletes the hawq config info by given cluster name.
func DeleteConfig(clusterName string) *common.ConfigService {
	req, err := http.NewRequest("DELETE", ServerURL+"/hawqconfig/"+clusterName, strings.NewReader(clusterName))
	req.Header.Set("Content-Type", restful.MIME_JSON)
	log.Print("Request in DeleteConfig is : ", req)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Print("Unexpected error in sending req: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		log.Print("Unexpected response: %s, expected: %s", resp.StatusCode, http.StatusOK)
	}
	return convertRespToConfig(resp)
}

// convertRespToConfig converts the response body to ConfigService struct.
func convertRespToConfig(resp *http.Response) *common.ConfigService {
	cs := new(common.ConfigService)
	hc := new(server.HAWQConfigResponse)
	if err := json.NewDecoder(resp.Body).Decode(hc); err != nil {
		log.Print("err occurred in json.NewDecoder ", err)
	}
	log.Print("Decode hc is : ", hc)
	if hc.Errno != 0 {
		log.Fatal("Unexpected response, error messages is %s", hc.Errmsg)
	}
	jsonStr, err := json.Marshal(hc.Data)
	err = json.Unmarshal(jsonStr, &cs)
	if err != nil {
		log.Print("Error occurred when json unmarshal configService : ", err)
	}
	log.Print("cs is", cs)
	log.Print("-------------------------")
	return cs
}
