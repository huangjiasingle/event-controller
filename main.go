package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"

	corev1 "k8s.io/api/core/v1"
	v1informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	kubeconfig    = flag.String("kubeconfig", "", "--kubeconfig")
	threadWorkNum = flag.Int("worknum", 0, "--worknum")
	etcdEndpoints = flag.String("etcdEndpoints", "", "--etcdEndpoints=127.0.0.1:2379,127.0.0.2:2379")
	etcdKeyPrefix = flag.String("etcdKeyPrefix", "", "--etcdKeyPrefix")
	k8sClient     *kubernetes.Clientset
	etcdClient    *clientv3.Client
	l             *log.Logger
	cfg           *rest.Config
	err           error
)

func init() {
	l = log.New(os.Stdout, "", log.Lshortfile|log.LstdFlags)
	l.Println(fmt.Sprintf("Go Version: %s", runtime.Version()))
	l.Println(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))

	flag.Parse()
	if *kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			panic(err.Error())
		}
	}
	k8sClient, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err.Error())
	}

	if *etcdEndpoints == "" {
		panic("etcdEndpoints flag must be set")
	}
	l.Println(strings.Split(*etcdEndpoints, ","))
	etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*etcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err.Error())
	}

	if *threadWorkNum == 0 {
		*threadWorkNum = 4
	}

	if *etcdKeyPrefix == "" {
		*etcdKeyPrefix = "/registry/events"
	}
}

func main() {
	// 控制并发的因素之一,通常需要结合 threadWorkNum 一起控制并发
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	queue := make(chan string, 50000)

	informer := v1informer.NewEventInformer(k8sClient, corev1.NamespaceAll, 0, cache.Indexers{})

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// 可以在这里直接调用写入etcd的方法写入etcd.为什么我不在这里直接操作呢. 考虑到event过多导致会并发的请求etcd. 从而导致etcd压力过大,写入数据失败. 当然可以做失败重试处理,这里我就不这么做了, 我做一个并发限速就行了
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err != nil {
				l.Println(err.Error())
				return
			}
			queue <- key
		},
	})

	ctl := &EventController{Informer: informer, Queue: queue}
	go ctl.processEvents()

	stop := make(chan struct{})
	defer close(stop)
	l.Println("started event conrtoller")
	go informer.Run(stop)
	<-stop
	l.Println("Stopping Pod controller")
}

func (c *EventController) processEvents() {
	for i := 0; i < *threadWorkNum; i++ {
		go c.runWork(i)
	}
}

func (c *EventController) runWork(id int) {
	for {
		select {
		case key := <-c.Queue:
			ev, exsit, err := c.Informer.GetIndexer().GetByKey(key)
			if err != nil || !exsit {
				continue
			}
			event := ev.(*corev1.Event)
			eventKey, err := cache.MetaNamespaceKeyFunc(event)
			if err != nil {
				l.Println(err.Error())
				continue
			}
			val, _ := json.Marshal(event)
			_, err = etcdClient.Put(context.TODO(), fmt.Sprintf("%v/%v", *etcdKeyPrefix, eventKey), string(val))
			if err != nil {
				l.Println(err.Error())
				continue
			}
			l.Println(fmt.Sprintf("add event to etcd successd: %v", eventKey))
		}
	}
}

// EventController event controller
type EventController struct {
	Informer cache.SharedIndexInformer
	Queue    chan string
}
