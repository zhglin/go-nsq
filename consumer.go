package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is the message processing interface for Consumer
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Consumer will automatically handle FINishing.
//
// When the returned value is non-nil Consumer will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
// 	consumer.AddHandler(nsq.HandlerFunc(func(m *Message) error {
// 		// handle the message
// 	}))
type HandlerFunc func(message *Message) error

// HandleMessage implements the Handler interface
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// DiscoveryFilter is an interface accepted by `SetBehaviorDelegate()`
// for filtering the nsqds returned from discovery via nsqlookupd
// 是一个被' SetBehaviorDelegate() '接受的接口，用于过滤通过nsqlookupd发现返回的nsqds
type DiscoveryFilter interface {
	Filter([]string) []string
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Consumer specified MaxAttemptCount)
// 是一个接口，当一个消息被认为是“失败”(即尝试的次数超过了Consumer指定的MaxAttemptCount)时，处理程序希望接收一个回调函数。
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

// ConsumerStats represents a snapshot of the state of a Consumer's connections and the messages
// it has seen
type ConsumerStats struct {
	MessagesReceived uint64
	MessagesFinished uint64
	MessagesRequeued uint64
	Connections      int
}

var instCount int64 // 初始的consumer的id号

type backoffSignal int

const (
	backoffFlag backoffSignal = iota
	continueFlag
	resumeFlag
)

// Consumer is a high-level type to consume from NSQ.
//
// A Consumer instance is supplied a Handler that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel. See: Handler/HandlerFunc
// for details on implementing the interface to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Consumer struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesReceived uint64 // 接收到消息的数量
	messagesFinished uint64 // fin消息数量
	messagesRequeued uint64 // req消息数量
	totalRdyCount    int64  // 已分配的所有链接总的Rdy
	backoffDuration  int64  // consumer回退的时间
	backoffCounter   int32  // message消费失败的次数
	maxInFlight      int32  // 最大消息消费数量 inFlight数量

	mtx sync.RWMutex

	logger   []logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	behaviorDelegate interface{} // 过滤lookupd发现nsqd的接口

	id      int64  // 唯一编号
	topic   string // topic名称
	channel string // channel名称
	config  Config // consumer配置

	rngMtx sync.Mutex
	rng    *rand.Rand // 随机数相关

	needRDYRedistributed int32 // 是否需要重新进行rdy分配

	backoffMtx sync.Mutex

	incomingMessages chan *Message // 接收到的消息

	rdyRetryMtx    sync.Mutex             // rdyRetryTimers更新锁
	rdyRetryTimers map[string]*time.Timer // 更新rdy的定时

	pendingConnections map[string]*Conn // 存放订阅之前的conn
	connections        map[string]*Conn // 存放完成订阅的conn

	nsqdTCPAddrs []string // 已连接的nsqd的地址

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string     // 以构建的查询lookupd的地址
	lookupdQueryIndex  int          // 轮询lookupdHTTPAddrs的下标
	lookupdHttpClient  *http.Client // 公用的lookupd客户端

	wg              sync.WaitGroup
	runningHandlers int32 // message回调的并发协程数
	stopFlag        int32 // 主动停止 先stop 后exit
	connectedFlag   int32 // 已调用ConnectTo的标记
	stopHandler     sync.Once
	exitHandler     sync.Once

	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int // 从该通道读取到阻塞，直到消费者完全停止 Stop()
	exitChan chan int // 退出信号 退出各个协程
}

// NewConsumer creates a new instance of Consumer for the specified topic/channel
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewConsumer the values are no longer mutable (they are copied).
// NewConsumer为指定的topic/channel创建一个新的Consumer实例
// 在Config被传递给NewConsumer之后，这些值不再是可变的(它们是被复制的)。
func NewConsumer(topic string, channel string, config *Config) (*Consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// topic，channel名称校验
	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	r := &Consumer{
		id: atomic.AddInt64(&instCount, 1),

		topic:   topic,
		channel: channel,
		config:  *config,

		logger:      make([]logger, LogLevelMax+1),
		logLvl:      LogLevelInfo,
		maxInFlight: int32(config.MaxInFlight),

		incomingMessages: make(chan *Message),

		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]*Conn),
		connections:        make(map[string]*Conn),

		lookupdRecheckChan: make(chan int, 1),

		rng: rand.New(rand.NewSource(time.Now().UnixNano())),

		StopChan: make(chan int),
		exitChan: make(chan int),
	}

	// Set default logger for all log levels
	l := log.New(os.Stderr, "", log.Flags())
	for index := range r.logger {
		r.logger[index] = l
	}

	r.wg.Add(1)
	go r.rdyLoop() // 定时分配rdy
	return r, nil
}

// Stats retrieves the current connection and message statistics for a Consumer
func (r *Consumer) Stats() *ConsumerStats {
	return &ConsumerStats{
		MessagesReceived: atomic.LoadUint64(&r.messagesReceived),
		MessagesFinished: atomic.LoadUint64(&r.messagesFinished),
		MessagesRequeued: atomic.LoadUint64(&r.messagesRequeued),
		Connections:      len(r.conns()),
	}
}

// 获取所有已订阅的链接
func (r *Consumer) conns() []*Conn {
	r.mtx.RLock()
	conns := make([]*Conn, 0, len(r.connections))
	for _, c := range r.connections {
		conns = append(conns, c)
	}
	r.mtx.RUnlock()
	return conns
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string) error
//
func (r *Consumer) SetLogger(l logger, lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	for level := range r.logger {
		r.logger[level] = l
	}
	r.logLvl = lvl
}

// SetLoggerForLevel assigns the same logger for specified `level`.
func (r *Consumer) SetLoggerForLevel(l logger, lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	r.logger[lvl] = l
}

// SetLoggerLevel sets the package logging level.
func (r *Consumer) SetLoggerLevel(lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	r.logLvl = lvl
}

func (r *Consumer) getLogger(lvl LogLevel) (logger, LogLevel) {
	r.logGuard.RLock()
	defer r.logGuard.RUnlock()

	return r.logger[lvl], r.logLvl
}

func (r *Consumer) getLogLevel() LogLevel {
	r.logGuard.RLock()
	defer r.logGuard.RUnlock()

	return r.logLvl
}

// SetBehaviorDelegate takes a type implementing one or more
// of the following interfaces that modify the behavior
// of the `Consumer`:
//
//    DiscoveryFilter
// SetBehaviorDelegate接受一个类型实现一个或多个接口，这些接口修改了' Consumer '的行为
func (r *Consumer) SetBehaviorDelegate(cb interface{}) {
	matched := false

	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	r.behaviorDelegate = cb
}

// perConnMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Consumer
// is responsible for.
// 计算每个连接的最大飞行数。
// 这可能会根据消费者负责的nsqd的连接数量而动态改变。
func (r *Consumer) perConnMaxInFlight() int64 {
	b := float64(r.getMaxInFlight()) // 获取consumer支持的最大inFlight消息数
	s := b / float64(len(r.conns())) // 计算平均值
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this consumer are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (r *Consumer) IsStarved() bool {
	for _, conn := range r.conns() {
		threshold := int64(float64(conn.RDY()) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && !conn.IsClosing() {
			return true
		}
	}
	return false
}

// 获取最大的inFlight消息数量
func (r *Consumer) getMaxInFlight() int32 {
	return atomic.LoadInt32(&r.maxInFlight)
}

// ChangeMaxInFlight sets a new maximum number of messages this comsumer instance
// will allow in-flight, and updates all existing connections as appropriate.
//
// For example, ChangeMaxInFlight(0) would pause message flow
//
// If already connected, it updates the reader RDY state for each connection.
// ChangeMaxInFlight设置此消费者实例将允许的最大消息数，并适当更新所有现有连接。
// 例如，ChangeMaxInFlight(0)将暂停消息流。如果已经连接，则更新每个连接的读取器RDY状态。
func (r *Consumer) ChangeMaxInFlight(maxInFlight int) {
	if r.getMaxInFlight() == int32(maxInFlight) {
		return
	}

	atomic.StoreInt32(&r.maxInFlight, int32(maxInFlight))

	for _, c := range r.conns() {
		r.maybeUpdateRDY(c)
	}
}

// SetLookupdHttpClient set lookupd http client
// 手动设置lookupdHttpClient
func (r *Consumer) SetLookupdHttpClient(httpclient *http.Client) {
	r.lookupdHttpClient = httpclient
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Consumer instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
// ConnectToNSQLookupd将nsqlookupd地址添加到该Consumer实例的列表中。
// 如果它是第一个添加的，它发起一个HTTP请求来发现配置主题的nsqd生产者。
// 生成一个goroutine来处理连续的轮询。
func (r *Consumer) ConnectToNSQLookupd(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}
	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	// 查询lookUp地址
	parsedAddr, err := buildLookupAddr(addr, r.topic)
	if err != nil {
		return err
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	// 是否已处理过
	r.mtx.Lock()
	for _, x := range r.lookupdHTTPAddrs {
		if x == parsedAddr {
			r.mtx.Unlock()
			return nil
		}
	}
	// 记录已处理的lookupd
	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs, parsedAddr)
	if r.lookupdHttpClient == nil {
		transport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   r.config.LookupdPollTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: r.config.LookupdPollTimeout,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
		}
		r.lookupdHttpClient = &http.Client{
			Transport: transport,
			Timeout:   r.config.LookupdPollTimeout,
		}
	}

	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.Unlock()

	// if this is the first one, kick off the go loop
	// 如果这是第一个，启动go循环
	if numLookupd == 1 {
		r.queryLookupd() // 查找nsqd服务器
		r.wg.Add(1)
		go r.lookupdLoop() // 定时查找nsqd服务器
	}

	return nil
}

// ConnectToNSQLookupds adds multiple nsqlookupd address to the list for this Consumer instance.
//
// If adding the first address it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
// ConnectToNSQLookupds将多个nsqlookupd地址添加到此Consumer实例的列表中。
// 如果添加第一个地址，它会发起一个HTTP请求来发现配置主题的nsqd生产者。
// 生成一个goroutine来处理连续的轮询。
func (r *Consumer) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval
// 每个LookupdPollInterval轮询所有已知的查找服务器
func (r *Consumer) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	// 添加一些抖动，以便多个用户发现同一主题时，当同时重启时，不会同时连接所有用户。
	r.rngMtx.Lock()
	jitter := time.Duration(int64(r.rng.Float64() *
		r.config.LookupdPollJitter * float64(r.config.LookupdPollInterval)))
	r.rngMtx.Unlock()
	var ticker *time.Ticker

	// 启动时间延后 防止同时启动
	select {
	case <-time.After(jitter):
	case <-r.exitChan:
		goto exit
	}

	ticker = time.NewTicker(r.config.LookupdPollInterval)

	for {
		select {
		case <-ticker.C:
			r.queryLookupd()
		case <-r.lookupdRecheckChan:
			r.queryLookupd()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	r.log(LogLevelInfo, "exiting lookupdLoop")
	r.wg.Done()
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
// 返回下一个查找端点以查询，并跟踪最后使用的端点
func (r *Consumer) nextLookupdEndpoint() string {
	r.mtx.RLock()
	if r.lookupdQueryIndex >= len(r.lookupdHTTPAddrs) {
		r.lookupdQueryIndex = 0
	}
	addr := r.lookupdHTTPAddrs[r.lookupdQueryIndex]
	num := len(r.lookupdHTTPAddrs)
	r.mtx.RUnlock()
	r.lookupdQueryIndex = (r.lookupdQueryIndex + 1) % num

	return addr
}

type lookupResp struct {
	Channels  []string    `json:"channels"`
	Producers []*peerInfo `json:"producers"`
	Timestamp int64       `json:"timestamp"`
}

type peerInfo struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// make an HTTP req to one of the configured nsqlookupd instances to discover
// which nsqd's provide the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
// 向配置的nsqlookupd实例之一发出HTTP请求，以发现哪个nsqd提供了我们正在使用的主题。发起与任何被识别的新生产者的连接。
func (r *Consumer) queryLookupd() {
	retries := 0

retry:
	endpoint := r.nextLookupdEndpoint()

	r.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	var data lookupResp
	headers := make(http.Header)
	if r.config.AuthSecret != "" && r.config.LookupdAuthorization {
		headers.Set("Authorization", fmt.Sprintf("Bearer %s", r.config.AuthSecret))
	}
	// 获取指定topic的nsqd
	err := apiRequestNegotiateV1(r.lookupdHttpClient, "GET", endpoint, headers, &data)
	if err != nil {
		r.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		retries++
		if retries < 3 {
			r.log(LogLevelInfo, "retrying with next nsqlookupd")
			goto retry
		}
		return
	}

	// 构建nsqd的地址
	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}

	// apply filter 应用过滤器
	if discoveryFilter, ok := r.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}

	// 链接nsqd跳过异常的节点
	for _, addr := range nsqdAddrs {
		err = r.ConnectToNSQD(addr)
		if err != nil && err != ErrAlreadyConnected {
			r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
			continue
		}
	}
}

// ConnectToNSQDs takes multiple nsqd addresses to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to local instance.
// 接受要直接连接到的多个NSQD地址。
// 建议使用ConnectToNSQLookupd，以便自动发现主题。
// 当您希望连接到本地实例时，此方法非常有用。
func (r *Consumer) ConnectToNSQDs(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQD(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectToNSQD takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
// ConnectToNSQD接受一个要直接连接的nsqd地址。
// 建议使用ConnectToNSQLookupd，以便自动发现主题。当您希望连接到单个本地实例时，此方法非常有用。
func (r *Consumer) ConnectToNSQD(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}

	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	// conn
	conn := NewConn(addr, &r.config, &consumerConnDelegate{r})
	conn.SetLoggerLevel(r.getLogLevel())
	format := fmt.Sprintf("%3d [%s/%s] (%%s)", r.id, r.topic, r.channel)
	for index := range r.logger {
		conn.SetLoggerForLevel(r.logger[index], LogLevel(index), format)
	}
	r.mtx.Lock()
	_, pendingOk := r.pendingConnections[addr]
	_, ok := r.connections[addr]
	if ok || pendingOk { // addr是否已存在
		r.mtx.Unlock()
		return ErrAlreadyConnected
	}
	r.pendingConnections[addr] = conn // 保存新创建的
	if idx := indexOf(addr, r.nsqdTCPAddrs); idx == -1 {
		r.nsqdTCPAddrs = append(r.nsqdTCPAddrs, addr)
	}
	r.mtx.Unlock()

	r.log(LogLevelInfo, "(%s) connecting to nsqd", addr)

	// 从pendingConnections清理的函数
	cleanupConnection := func() {
		r.mtx.Lock()
		delete(r.pendingConnections, addr)
		r.mtx.Unlock()
		conn.Close()
	}

	// 建立链接 发送identify请求
	resp, err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}

	if resp != nil {
		if resp.MaxRdyCount < int64(r.getMaxInFlight()) {
			r.log(LogLevelWarning,
				"(%s) max RDY count %d < consumer max in flight %d, truncation possible",
				conn.String(), resp.MaxRdyCount, r.getMaxInFlight())
		}
	}

	// 订阅topic，channel
	cmd := Subscribe(r.topic, r.channel)
	err = conn.WriteCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s",
			conn, r.topic, r.channel, err.Error())
	}

	// 已完成订阅的链接
	r.mtx.Lock()
	delete(r.pendingConnections, addr)
	r.connections[addr] = conn
	r.mtx.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	// 抢占信号到现有的连接，以降低其RDY计数
	for _, c := range r.conns() {
		if c != conn {
			r.maybeUpdateRDY(c)
		}
	}
	r.maybeUpdateRDY(conn)

	return nil
}

func indexOf(n string, h []string) int {
	for i, a := range h {
		if n == a {
			return i
		}
	}
	return -1
}

// DisconnectFromNSQD closes the connection to and removes the specified
// `nsqd` address from the list
// 关闭到列表的连接并从列表中删除指定的“nsqd”地址，否则被关闭的链接会重新链接
func (r *Consumer) DisconnectFromNSQD(addr string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOf(addr, r.nsqdTCPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	// slice delete
	r.nsqdTCPAddrs = append(r.nsqdTCPAddrs[:idx], r.nsqdTCPAddrs[idx+1:]...)

	pendingConn, pendingOk := r.pendingConnections[addr]
	conn, ok := r.connections[addr]

	if ok {
		conn.Close()
	} else if pendingOk {
		pendingConn.Close()
	}

	return nil
}

// DisconnectFromNSQLookupd removes the specified `nsqlookupd` address
// from the list used for periodic discovery.
func (r *Consumer) DisconnectFromNSQLookupd(addr string) error {
	parsedAddr, err := buildLookupAddr(addr, r.topic)
	if err != nil {
		return err
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOf(parsedAddr, r.lookupdHTTPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	if len(r.lookupdHTTPAddrs) == 1 {
		return fmt.Errorf("cannot disconnect from only remaining nsqlookupd HTTP address %s", addr)
	}

	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs[:idx], r.lookupdHTTPAddrs[idx+1:]...)

	return nil
}

// 接收到消息回调
func (r *Consumer) onConnMessage(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesReceived, 1)
	r.incomingMessages <- msg
}

func (r *Consumer) onConnMessageFinished(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesFinished, 1)
}

func (r *Consumer) onConnMessageRequeued(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesRequeued, 1)
}

// message消费失败进行回退
func (r *Consumer) onConnBackoff(c *Conn) {
	r.startStopContinueBackoff(c, backoffFlag)
}

// message消费失败跳过不回退
func (r *Consumer) onConnContinue(c *Conn) {
	r.startStopContinueBackoff(c, continueFlag)
}

// 消费成功关闭回退
func (r *Consumer) onConnResume(c *Conn) {
	r.startStopContinueBackoff(c, resumeFlag)
}

// 响应的回调 只处理close 关闭链接的读
func (r *Consumer) onConnResponse(c *Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		// server is ready for us to close (it ack'd our StartClose)
		// we can assume we will not receive any more messages over this channel
		// (but we can still write back responses)
		// 服务器已经准备好让我们关闭(它确认了我们的StartClose)
		// 我们可以假设不再通过该通道接收任何消息(但仍然可以写回响应)
		r.log(LogLevelInfo, "(%s) received CLOSE_WAIT from nsqd", c.String())
		c.Close()
	}
}

// FrameTypeError的报文
func (r *Consumer) onConnError(c *Conn, data []byte) {}

// consumer收到心跳包
func (r *Consumer) onConnHeartbeat(c *Conn) {}

// tcp链接读写的错误调用，之后会调用c.close关闭链接，关闭readLoop，writeLoop
func (r *Consumer) onConnIOError(c *Conn, err error) {
	c.Close() // 只关闭读
}

// 链接关闭后的回调
func (r *Consumer) onConnClose(c *Conn) {
	var hasRDYRetryTimer bool

	// remove this connections RDY count from the consumer's total
	// 从使用者的总数中删除此连接RDY计数
	rdyCount := c.RDY()
	atomic.AddInt64(&r.totalRdyCount, -rdyCount)

	// 删除定时更新
	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.String()]; ok {
		// stop any pending retry of an old RDY update
		timer.Stop()
		delete(r.rdyRetryTimers, c.String())
		hasRDYRetryTimer = true
	}
	r.rdyRetryMtx.Unlock()

	// 删除链接
	r.mtx.Lock()
	delete(r.connections, c.String())
	left := len(r.connections)
	r.mtx.Unlock()

	r.log(LogLevelWarning, "there are %d connections left alive", left)

	if (hasRDYRetryTimer || rdyCount > 0) &&
		(int32(left) == r.getMaxInFlight() || r.inBackoff()) {
		// we're toggling out of (normal) redistribution cases and this conn
		// had a RDY count...
		//
		// trigger RDY redistribution to make sure this RDY is moved
		// to a new connection
		// 我们正在切换(正常)重新分配的情况，这个conn有一个RDY计数…
		// 触发RDY再分配，以确保该RDY被移动到一个新的连接
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	// we were the last one (and stopping)
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		if left == 0 {
			r.stopHandlers()
		}
		return
	}

	r.mtx.RLock()
	numLookupd := len(r.lookupdHTTPAddrs)
	reconnect := indexOf(c.String(), r.nsqdTCPAddrs) >= 0 // 非主动关闭的，重新连接
	r.mtx.RUnlock()
	if numLookupd > 0 {
		// trigger a poll of the lookupd 触发对被查询对象的轮询，可能是nsqd服务问题
		select {
		case r.lookupdRecheckChan <- 1:
		default:
		}
	} else if reconnect {
		// there are no lookupd and we still have this nsqd TCP address in our list...
		// try to reconnect after a bit
		// 没有查找，我们的列表中仍然有这个nsqd TCP地址…稍候再尝试重新连接
		go func(addr string) {
			for {
				r.log(LogLevelInfo, "(%s) re-connecting in %s", addr, r.config.LookupdPollInterval)
				time.Sleep(r.config.LookupdPollInterval)
				if atomic.LoadInt32(&r.stopFlag) == 1 { // 已退出了
					break
				}
				// 重新校验是否手动删掉了
				r.mtx.RLock()
				reconnect := indexOf(addr, r.nsqdTCPAddrs) >= 0
				r.mtx.RUnlock()
				if !reconnect {
					r.log(LogLevelWarning, "(%s) skipped reconnect after removal...", addr)
					return
				}
				// 链接
				err := r.ConnectToNSQD(addr)
				if err != nil && err != ErrAlreadyConnected {
					r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
				break
			}
		}(c.String())
	}
}

// 动态调整各个链接的InFlight数量 因消息消费失败导致
func (r *Consumer) startStopContinueBackoff(conn *Conn, signal backoffSignal) {
	// prevent many async failures/successes from immediately resulting in
	// max backoff/normal rate (by ensuring that we dont continually incr/decr
	// the counter during a backoff period)
	// 防止许多异步失败/成功立即导致最大回退/正常速率(通过确保我们在回退期间不不断incr/decr计数器)
	r.backoffMtx.Lock()
	defer r.backoffMtx.Unlock()
	// 已经在回退中跳过
	if r.inBackoffTimeout() {
		return
	}

	// update backoff state 更新回退状态
	backoffUpdated := false
	backoffCounter := atomic.LoadInt32(&r.backoffCounter)
	switch signal {
	case resumeFlag: // message消费成功 递减
		if backoffCounter > 0 {
			backoffCounter--
			backoffUpdated = true
		}
	case backoffFlag: // message消费失败 递增 计算回退时间
		nextBackoff := r.config.BackoffStrategy.Calculate(int(backoffCounter) + 1)
		if nextBackoff <= r.config.MaxBackoffDuration {
			backoffCounter++
			backoffUpdated = true
		}
	}
	atomic.StoreInt32(&r.backoffCounter, backoffCounter)

	if r.backoffCounter == 0 && backoffUpdated {
		// exit backoff 退出后退
		count := r.perConnMaxInFlight()
		r.log(LogLevelWarning, "exiting backoff, returning all to RDY %d", count)
		for _, c := range r.conns() {
			r.updateRDY(c, count)
		}
	} else if r.backoffCounter > 0 {
		// start or continue backoff 计算后退时间并开始或继续后退
		backoffDuration := r.config.BackoffStrategy.Calculate(int(backoffCounter))

		if backoffDuration > r.config.MaxBackoffDuration {
			backoffDuration = r.config.MaxBackoffDuration
		}

		r.log(LogLevelWarning, "backing off for %s (backoff level %d), setting all to RDY 0",
			backoffDuration, backoffCounter)

		// send RDY 0 immediately (to *all* connections)
		// 立即发送RDY 0(到*所有*连接)
		for _, c := range r.conns() {
			r.updateRDY(c, 0)
		}

		// 等待
		r.backoff(backoffDuration)
	}
}

// 定时关闭或减少回退时间
func (r *Consumer) backoff(d time.Duration) {
	atomic.StoreInt64(&r.backoffDuration, d.Nanoseconds())
	time.AfterFunc(d, r.resume) // 不会阻塞
}

// 关闭或减少回退
func (r *Consumer) resume() {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		atomic.StoreInt64(&r.backoffDuration, 0)
		return
	}

	// pick a random connection to test the waters
	// 选择一个随机的连接来测试一下
	conns := r.conns()
	if len(conns) == 0 {
		r.log(LogLevelWarning, "no connection available to resume")
		r.log(LogLevelWarning, "backing off for %s", time.Second)
		r.backoff(time.Second)
		return
	}
	r.rngMtx.Lock()
	idx := r.rng.Intn(len(conns))
	r.rngMtx.Unlock()
	choice := conns[idx]

	r.log(LogLevelWarning,
		"(%s) backoff timeout expired, sending RDY 1",
		choice.String())

	// while in backoff only ever let 1 message at a time through
	// 在后退时，一次只能让一条信息通过
	err := r.updateRDY(choice, 1)
	if err != nil {
		r.log(LogLevelWarning, "(%s) error resuming RDY 1 - %s", choice.String(), err)
		r.log(LogLevelWarning, "backing off for %s", time.Second)
		r.backoff(time.Second)
		return
	}

	// 更新回退时间
	atomic.StoreInt64(&r.backoffDuration, 0)
}

// 当前message消费失败的次数
func (r *Consumer) inBackoff() bool {
	return atomic.LoadInt32(&r.backoffCounter) > 0
}

// 是否回退中
func (r *Consumer) inBackoffTimeout() bool {
	return atomic.LoadInt64(&r.backoffDuration) > 0
}

// 更新每个链接的InFlight
func (r *Consumer) maybeUpdateRDY(conn *Conn) {
	inBackoff := r.inBackoff()
	inBackoffTimeout := r.inBackoffTimeout()
	if inBackoff || inBackoffTimeout {
		r.log(LogLevelDebug, "(%s) skip sending RDY inBackoff:%v || inBackoffTimeout:%v",
			conn, inBackoff, inBackoffTimeout)
		return
	}

	count := r.perConnMaxInFlight()
	r.log(LogLevelDebug, "(%s) sending RDY %d", conn, count)
	if err := r.updateRDY(conn, count); err != nil {
		r.log(LogLevelWarning, "(%s) error sending RDY %d: %v", conn, count, err)
	}
}

// 定时重分配rdy数量
func (r *Consumer) rdyLoop() {
	redistributeTicker := time.NewTicker(r.config.RDYRedistributeInterval)

	for {
		select {
		case <-redistributeTicker.C:
			r.redistributeRDY()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	redistributeTicker.Stop()
	r.log(LogLevelInfo, "rdyLoop exiting")
	r.wg.Done()
}

// 设置RDY的数量
func (r *Consumer) updateRDY(c *Conn, count int64) error {
	if c.IsClosing() {
		return ErrClosing
	}

	// never exceed the nsqd's configured max RDY count
	// 不要超过nsqd配置的最大RDY计数
	if count > c.MaxRDY() {
		count = c.MaxRDY()
	}

	// stop any pending retry of an old RDY update
	// 停止任何等待的旧RDY更新重试
	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.String()]; ok {
		timer.Stop()
		delete(r.rdyRetryTimers, c.String())
	}
	r.rdyRetryMtx.Unlock()

	// never exceed our global max in flight. truncate if possible.
	// this could help a new connection get partial max-in-flight
	// 飞行中消息数量永远不要超过我们的全球最大限额。如果可能的话,截断。这可以帮助新连接获得部分最大飞行数
	rdyCount := c.RDY()
	// 当前链接还能分配的最大rdy数量
	maxPossibleRdy := int64(r.getMaxInFlight()) - atomic.LoadInt64(&r.totalRdyCount) + rdyCount
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	// 超过限额
	if maxPossibleRdy <= 0 && count > 0 {
		if rdyCount == 0 {
			// we wanted to exit a zero RDY count but we couldn't send it...
			// in order to prevent eternal starvation we reschedule this attempt
			// (if any other RDY update succeeds this timer will be stopped)
			// 我们想退出一个零的RDY计数，但我们不能发送它…
			// 为了防止永远的饥饿，我们重新安排了这个尝试(如果任何其他RDY更新成功，这个计时器将停止)
			r.rdyRetryMtx.Lock()
			r.rdyRetryTimers[c.String()] = time.AfterFunc(5*time.Second,
				func() {
					r.updateRDY(c, count)
				})
			r.rdyRetryMtx.Unlock()
		}
		return ErrOverMaxInFlight
	}

	// 设置rdy
	return r.sendRDY(c, count)
}

// 设置链接rdy数量
func (r *Consumer) sendRDY(c *Conn, count int64) error {
	if count == 0 && c.LastRDY() == 0 {
		// no need to send. It's already that RDY count
		return nil
	}

	// 增加分配数量
	atomic.AddInt64(&r.totalRdyCount, count-c.RDY())

	// 记录
	lastRDY := c.LastRDY()
	c.SetRDY(count)
	if count == lastRDY {
		return nil
	}

	// 发送command命令
	err := c.WriteCommand(Ready(int(count)))
	if err != nil {
		r.log(LogLevelError, "(%s) error sending RDY %d - %s", c.String(), count, err)
		return err
	}
	return nil
}

// 重新分配rdy数量
func (r *Consumer) redistributeRDY() {
	// 回退中跳过
	if r.inBackoffTimeout() {
		return
	}

	// if an external heuristic set needRDYRedistributed we want to wait
	// until we can actually redistribute to proceed
	// 如果一个外部启发式设置为needdrdyredistributed，我们想要等待，直到我们真正可以重新分配
	conns := r.conns()
	if len(conns) == 0 {
		return
	}

	// 这种情况会有链接无法消费消息
	maxInFlight := r.getMaxInFlight()
	if len(conns) > int(maxInFlight) {
		r.log(LogLevelDebug, "redistributing RDY state (%d conns > %d max_in_flight)",
			len(conns), maxInFlight)
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	if r.inBackoff() && len(conns) > 1 {
		r.log(LogLevelDebug, "redistributing RDY state (in backoff and %d conns > 1)", len(conns))
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	// 是否需要进行调整
	if !atomic.CompareAndSwapInt32(&r.needRDYRedistributed, 1, 0) {
		return
	}

	possibleConns := make([]*Conn, 0, len(conns))
	for _, c := range conns {
		lastMsgDuration := time.Now().Sub(c.LastMessageTime())
		lastRdyDuration := time.Now().Sub(c.LastRdyTime())
		rdyCount := c.RDY()
		r.log(LogLevelDebug, "(%s) rdy: %d (last message received %s)",
			c.String(), rdyCount, lastMsgDuration)
		if rdyCount > 0 { // rdy大于0的情况下
			if lastMsgDuration > r.config.LowRdyIdleTimeout { // 长时间未收到消息
				r.log(LogLevelDebug, "(%s) idle connection, giving up RDY", c.String())
				r.updateRDY(c, 0)
			} else if lastRdyDuration > r.config.LowRdyTimeout { // 长时间未更新rdy
				r.log(LogLevelDebug, "(%s) RDY timeout, giving up RDY", c.String())
				r.updateRDY(c, 0)
			}
		}
		possibleConns = append(possibleConns, c)
	}

	// 剩余可重新分配的数量
	availableMaxInFlight := int64(maxInFlight) - atomic.LoadInt64(&r.totalRdyCount)
	if r.inBackoff() {
		availableMaxInFlight = 1 - atomic.LoadInt64(&r.totalRdyCount)
	}

	for len(possibleConns) > 0 && availableMaxInFlight > 0 {
		availableMaxInFlight--
		r.rngMtx.Lock()
		i := r.rng.Int() % len(possibleConns)
		r.rngMtx.Unlock()
		c := possibleConns[i]
		// delete
		possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
		r.log(LogLevelDebug, "(%s) redistributing RDY", c.String())
		r.updateRDY(c, 1)
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
// Stop将启动Consumer的优雅停止(永久)
// 注意:接收在StopChan上阻塞，直到这个过程完成
// 服务端不在下发消息
func (r *Consumer) Stop() {
	if !atomic.CompareAndSwapInt32(&r.stopFlag, 0, 1) {
		return
	}

	r.log(LogLevelInfo, "stopping...")

	// 所有链接已关闭
	if len(r.conns()) == 0 {
		r.stopHandlers()
	} else { // 关闭所有链接
		for _, c := range r.conns() {
			err := c.WriteCommand(StartClose())
			if err != nil {
				r.log(LogLevelError, "(%s) error sending CLS - %s", c.String(), err)
			}
		}

		// 等待所有的conn关闭
		time.AfterFunc(time.Second*30, func() {
			// if we've waited this long handlers are blocked on processing messages
			// so we can't just stopHandlers (if any adtl. messages were pending processing
			// we would cause a panic on channel close)
			//
			// instead, we just bypass handler closing and skip to the final exit
			r.exit()
		})
	}
}

// 关闭incomingMessages
func (r *Consumer) stopHandlers() {
	r.stopHandler.Do(func() {
		r.log(LogLevelInfo, "stopping handlers")
		close(r.incomingMessages)
	})
}

// AddHandler sets the Handler for messages received by this Consumer. This can be called
// multiple times to add additional handlers. Handler will have a 1:1 ratio to message handling goroutines.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
// 为这个消费者接收的消息设置处理程序。这可以多次调用以添加其他处理程序。处理程序与消息处理goroutines的比率为1:1。
// 连接到NSQD或NSQ Lookupd后调用，将会出现异常(参见Handler或HandlerFunc了解实现该接口的详细信息)
func (r *Consumer) AddHandler(handler Handler) {
	r.AddConcurrentHandlers(handler, 1)
}

// AddConcurrentHandlers sets the Handler for messages received by this Consumer.  It
// takes a second argument which indicates the number of goroutines to spawn for
// message handling.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
// AddConcurrentHandlers设置接收到的消息的处理程序。
// 它接受第二个参数，该参数指示为消息处理而生成的goroute的数量。
// 如果在连接到NSQD或NSQLookupd之后调用，则会出现异常(有关实现该接口的详细信息，请参阅Handler或HandlerFunc)
func (r *Consumer) AddConcurrentHandlers(handler Handler, concurrency int) {
	if atomic.LoadInt32(&r.connectedFlag) == 1 {
		panic("already connected")
	}

	atomic.AddInt32(&r.runningHandlers, int32(concurrency))
	for i := 0; i < concurrency; i++ {
		go r.handlerLoop(handler)
	}
}

func (r *Consumer) handlerLoop(handler Handler) {
	r.log(LogLevelDebug, "starting Handler")

	for {
		message, ok := <-r.incomingMessages
		if !ok {
			goto exit
		}

		if r.shouldFailMessage(message, handler) {
			message.Finish()
			continue
		}

		err := handler.HandleMessage(message)
		if err != nil {
			r.log(LogLevelError, "Handler returned error (%s) for msg %s", err, message.ID)
			if !message.IsAutoResponseDisabled() {
				message.Requeue(-1)
			}
			continue
		}

		if !message.IsAutoResponseDisabled() {
			message.Finish()
		}
	}

exit:
	r.log(LogLevelDebug, "stopping Handler")
	if atomic.AddInt32(&r.runningHandlers, -1) == 0 {
		r.exit()
	}
}

// 是否是重试次数过多的消息
func (r *Consumer) shouldFailMessage(message *Message, handler interface{}) bool {
	// message passed the max number of attempts 消息传递的最大尝试次数
	if r.config.MaxAttempts > 0 && message.Attempts > r.config.MaxAttempts {
		r.log(LogLevelWarning, "msg %s attempted %d times, giving up",
			message.ID, message.Attempts)

		// 回调
		logger, ok := handler.(FailedMessageLogger)
		if ok {
			logger.LogFailedMessage(message)
		}

		return true
	}
	return false
}

// consumer退出
func (r *Consumer) exit() {
	r.exitHandler.Do(func() {
		close(r.exitChan)
		r.wg.Wait()
		close(r.StopChan)
	})
}

func (r *Consumer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := r.getLogger(lvl)

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d [%s/%s] %s",
		lvl, r.id, r.topic, r.channel,
		fmt.Sprintf(line, args...)))
}

// 构造lookUp的http地址
func buildLookupAddr(addr, topic string) (string, error) {
	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return "", err
	}

	if u.Port() == "" {
		return "", errors.New("missing port")
	}

	if u.Path == "/" || u.Path == "" {
		u.Path = "/lookup"
	}

	v, err := url.ParseQuery(u.RawQuery)
	v.Add("topic", topic)
	u.RawQuery = v.Encode()
	return u.String(), nil
}
