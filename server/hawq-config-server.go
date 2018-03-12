package server

import (
	"github.com/emicklei/go-restful"
	"log"
	"net/http"
)

const (
	ErrSameClusterName         = 100
	ErrClusterNotFound         = 101
	ErrUpdateSysParamForbidden = 102
	ErrClusterNameNotExist     = 103
	ErrDeleteClusterFailed     = 104
)

var errorMap = map[int]string{
	ErrSameClusterName:         "Same cluster name",
	ErrClusterNotFound:         "Can not find the cluster",
	ErrUpdateSysParamForbidden: "Can not update system params",
	ErrClusterNameNotExist:     "Cluster name not exists",
	ErrDeleteClusterFailed:     "Delete cluster failed",
}

type HAWQConfigRequest struct {
	ClusterName string `json: "cluster_name"`
}

type HAWQConfigResponse struct {
	Errno  int
	Errmsg string
	Data   interface{}
}

// Register initializes the route methods e.g. GET, POST, PUT, DELETE.
func (hcresp HAWQConfigResponse) Register(container *restful.Container) {
	ws := new(restful.WebService)
	ws.
		Path("/hawqconfig").
		Consumes(restful.MIME_XML, restful.MIME_JSON). //restful.MIME_XML or restful.MIME_JSON
		Produces(restful.MIME_JSON, restful.MIME_XML)

	ws.Route(ws.GET("/{cluster_name}").To(hcresp.findCluster))
	ws.Route(ws.POST("/{cluster_name}").To(hcresp.updateCluster))
	ws.Route(ws.PUT("/{cluster_name}").To(hcresp.createCluster))
	ws.Route(ws.DELETE("/{cluster_name}").To(hcresp.removeCluster))

	container.Add(ws)
}

// GET http://localhost:8090/hawqconfig/1
func (hcresp HAWQConfigResponse) findCluster(request *restful.Request, response *restful.Response) {
	hcreq := new(HAWQConfigRequest)
	hcreq.ClusterName = request.PathParameter("cluster_name")
	hcresp.Data = getConfigFromEtcd(hcreq.ClusterName)
	if hcresp.Data == "" {
		hcresp.Errno = ErrClusterNotFound
		hcresp.Errmsg = errorMap[ErrClusterNotFound]
	}
	log.Print("hcresp in findCluster is : ", hcresp)
	response.WriteEntity(hcresp)
	//response.AddHeader("Content-Type", "text/plain")
	//response.WriteErrorString(http.StatusNotFound, "Cluster name could not be found.")
}

// POST http://localhost:8090/hawqconfig
func (hcresp *HAWQConfigResponse) updateCluster(request *restful.Request, response *restful.Response) {
	//configMap := make(map[string]interface{})
	hcreq := new(HAWQConfigRequest)
	hcreq.ClusterName = request.PathParameter("cluster_name")
	log.Print("hcreq in createCluster is :", hcreq.ClusterName)
	configMap := make(map[string]string)
	err := request.ReadEntity(&configMap)
	if err != nil {
		log.Print("err in updateCluster when request.ReadEntity is : ", err)
	}
	log.Print("configMap in updateCluster is :", configMap)
	if isSysParam(configMap) {
		hcresp.Errno = ErrUpdateSysParamForbidden
		hcresp.Errmsg = errorMap[ErrUpdateSysParamForbidden]
	} else {
		hcInfo := getConfigFromEtcd(hcreq.ClusterName)
		hcInfo = updateHAWQConfigInfo(hcInfo, configMap)
		storeToEtcd(hcreq.ClusterName, hcInfo)
		hcresp.Data = hcInfo
	}
	log.Print("hcresp in updateCluster is", hcresp.Data)
	response.WriteEntity(hcresp)
}

// PUT http://localhost:8090/hawconfig/1
func (hcresp *HAWQConfigResponse) createCluster(request *restful.Request, response *restful.Response) {
	hcreq := new(HAWQConfigRequest)
	hcreq.ClusterName = request.PathParameter("cluster_name")
	log.Print("hcreq in createCluster is :", hcreq.ClusterName, hcreq)
	//check cluster name exist or not
	if checkEtcdKeyExists(hcreq.ClusterName) == true {
		hcresp.Errno = ErrSameClusterName
		hcresp.Errmsg = errorMap[ErrSameClusterName]
		log.Print("hcresp.errno hcresp.errmsg is ", hcresp.Errno, hcresp.Errmsg)
	} else {
		hcInfo := generateHAWQConfigInfo(hcreq.ClusterName)
		//hcInfo, err := generateHAWQConfigInfo(hcreq.ClusterName)
		hcresp.Data = hcInfo
		storeToEtcd(hcreq.ClusterName, hcInfo)
		response.WriteHeader(http.StatusCreated)
	}
	log.Print("hcresp in createCluster is", hcresp.Data)
	response.WriteEntity(hcresp)
	//response.AddHeader("Content-Type", "text/plain")
	//response.WriteErrorString(http.StatusInternalServerError, err.Error())
	//storeToEtcd()
}

// DELETE http://localhost:8090/hawqconfig/1
func (hcresp *HAWQConfigResponse) removeCluster(request *restful.Request, response *restful.Response) {
	hcreq := new(HAWQConfigRequest)
	hcreq.ClusterName = request.PathParameter("cluster_name")
	log.Print("hcreq in createCluster is :", hcreq.ClusterName, hcreq)
	//check cluster name exist or not
	if checkEtcdKeyExists(hcreq.ClusterName) != true {
		hcresp.Errno = ErrClusterNameNotExist
		hcresp.Errmsg = errorMap[ErrClusterNameNotExist]
		log.Print("hcresp.errno hcresp.errmsg is ", hcresp.Errno, hcresp.Errmsg)
	} else {
		delFlag := delFromEtcd(hcreq.ClusterName)
		if delFlag != true {
			hcresp.Errno = ErrDeleteClusterFailed
			hcresp.Errmsg = errorMap[ErrDeleteClusterFailed]
			log.Print("hcresp.errno hcresp.errmsg is ", hcresp.Errno, hcresp.Errmsg)
		}
	}
	log.Print("hcresp in deleteCluster is", hcresp.Data)
	response.WriteEntity(hcresp)
}

// RunRestfulCurlyRouterServer starts a curl server.
func RunRestfulCurlyRouterServer() {
	wsContainer := restful.NewContainer()
	wsContainer.Router(restful.CurlyRouter{})
	hcresp := new(HAWQConfigResponse)
	hcresp.Register(wsContainer)

	log.Print("start listening on localhost:8090")
	server := &http.Server{Addr: ":8090", Handler: wsContainer}
	log.Fatal(server.ListenAndServe())
}
