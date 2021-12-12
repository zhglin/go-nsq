package nsq

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
)

// IdentifyResponse represents the metadata
// returned from an IDENTIFY command to nsqd
type IdentifyResponse struct {
	MaxRdyCount  int64 `json:"max_rdy_count"`
	TLSv1        bool  `json:"tls_v1"`
	Deflate      bool  `json:"deflate"`
	Snappy       bool  `json:"snappy"`
	AuthRequired bool  `json:"auth_required"`
}

// AuthResponse represents the metadata
// returned from an AUTH command to nsqd
// 表示从AUTH命令返回到nsqd的元数据
type AuthResponse struct {
	Identity        string `json:"identity"`
	IdentityUrl     string `json:"identity_url"`
	PermissionCount int64  `json:"permission_count"`
}

//  message的响应信息
type msgResponse struct {
	msg     *Message // 原始信息
	cmd     *Command // 需要发送个nsqd的命令
	success bool     // 是否消费成功
	backoff bool
}

// Conn represents a connection to nsqd
//
// Conn exposes a set of callbacks for the
// various events that occur on a connection
// Conn表示到nsqd的连接
// Conn为连接上发生的各种事件公开一组回调
type Conn struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesInFlight int64 // inFlight消息数量
	maxRdyCount      int64 // 准备接收消息的数量 nsqd返回的配置
	rdyCount         int64 // 当前链接的rdy数量
	lastRdyTimestamp int64
	lastMsgTimestamp int64 // 接收到最后一条message的时间

	mtx sync.Mutex

	config *Config // 配置

	conn    *net.TCPConn
	tlsConn *tls.Conn
	addr    string // 连接地址

	delegate ConnDelegate // 各种回调

	logger   []logger
	logLvl   LogLevel
	logFmt   []string
	logGuard sync.RWMutex

	r io.Reader // tcp链接
	w io.Writer // tcp链接

	cmdChan         chan *Command     // 需要想nsqd发送的命令
	msgResponseChan chan *msgResponse // message需要响应给nsqd的命令
	exitChan        chan int
	drainReady      chan int

	closeFlag int32 // 关闭tcp链接的标记，不在读取tcp数据
	stopper   sync.Once
	wg        sync.WaitGroup

	readLoopRunning int32 // 标记是否循环读取tcp
}

// NewConn returns a new Conn instance
// 返回一个新的Conn实例
func NewConn(addr string, config *Config, delegate ConnDelegate) *Conn {
	if !config.initialized {
		panic("Config must be created with NewConfig()")
	}
	return &Conn{
		addr: addr,

		config:   config,
		delegate: delegate,

		maxRdyCount:      2500,
		lastMsgTimestamp: time.Now().UnixNano(),

		cmdChan:         make(chan *Command),
		msgResponseChan: make(chan *msgResponse),
		exitChan:        make(chan int),
		drainReady:      make(chan int),

		logger: make([]logger, LogLevelMax+1),
		logFmt: make([]string, LogLevelMax+1),
	}
}

// SetLogger assigns the logger to use as well as a level.
//
// The format parameter is expected to be a printf compatible string with
// a single %s argument.  This is useful if you want to provide additional
// context to the log messages that the connection will print, the default
// is '(%s)'.
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (c *Conn) SetLogger(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	if format == "" {
		format = "(%s)"
	}
	for level := range c.logger {
		c.logger[level] = l
		c.logFmt[level] = format
	}
	c.logLvl = lvl
}

func (c *Conn) SetLoggerForLevel(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	if format == "" {
		format = "(%s)"
	}
	c.logger[lvl] = l
	c.logFmt[lvl] = format
}

// SetLoggerLevel sets the package logging level.
func (c *Conn) SetLoggerLevel(lvl LogLevel) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	c.logLvl = lvl
}

func (c *Conn) getLogger(lvl LogLevel) (logger, LogLevel, string) {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logger[lvl], c.logLvl, c.logFmt[lvl]
}

func (c *Conn) getLogLevel() LogLevel {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logLvl
}

// Connect dials and bootstraps the nsqd connection
// (including IDENTIFY) and returns the IdentifyResponse
// nsqd连接(包括IDENTIFY)并返回IdentifyResponse
func (c *Conn) Connect() (*IdentifyResponse, error) {
	dialer := &net.Dialer{
		LocalAddr: c.config.LocalAddr,
		Timeout:   c.config.DialTimeout,
	}

	conn, err := dialer.Dial("tcp", c.addr)
	if err != nil {
		return nil, err
	}
	c.conn = conn.(*net.TCPConn) // 强制类型转换
	c.r = conn
	c.w = conn

	_, err = c.Write(MagicV2) // 发送协议版本号
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
	}

	// 发送并接收identify请求以及相应
	resp, err := c.identify()
	if err != nil {
		return nil, err
	}

	// 必须需要进行auth认证
	if resp != nil && resp.AuthRequired {
		if c.config.AuthSecret == "" {
			c.log(LogLevelError, "Auth Required")
			return nil, errors.New("Auth Required")
		}
		// 发送auth认证
		err := c.auth(c.config.AuthSecret)
		if err != nil {
			c.log(LogLevelError, "Auth Failed %s", err)
			return nil, err
		}
	}

	c.wg.Add(2)
	atomic.StoreInt32(&c.readLoopRunning, 1)
	go c.readLoop()  // 异步读取nsqd响应
	go c.writeLoop() // 异步写入command
	return resp, nil
}

// Close idempotently initiates connection close
// 幂等地启动连接关闭
func (c *Conn) Close() error {
	atomic.StoreInt32(&c.closeFlag, 1)
	if c.conn != nil && atomic.LoadInt64(&c.messagesInFlight) == 0 {
		return c.conn.CloseRead() // 关闭TCP连接的读取
	}
	return nil
}

// IsClosing indicates whether or not the
// connection is currently in the processing of
// gracefully closing
func (c *Conn) IsClosing() bool {
	return atomic.LoadInt32(&c.closeFlag) == 1
}

// RDY returns the current RDY count
// 返回当前RDY计数
func (c *Conn) RDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

// LastRDY returns the previously set RDY count
func (c *Conn) LastRDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

// SetRDY stores the specified RDY count
func (c *Conn) SetRDY(rdy int64) {
	atomic.StoreInt64(&c.rdyCount, rdy)
	if rdy > 0 {
		atomic.StoreInt64(&c.lastRdyTimestamp, time.Now().UnixNano())
	}
}

// MaxRDY returns the nsqd negotiated maximum
// RDY count that it will accept for this connection
// 返回该连接将接受的nsqd协商的最大RDY计数
func (c *Conn) MaxRDY() int64 {
	return c.maxRdyCount
}

// LastRdyTime returns the time of the last non-zero RDY
// update for this connection
func (c *Conn) LastRdyTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastRdyTimestamp))
}

// LastMessageTime returns a time.Time representing
// the time at which the last message was received
func (c *Conn) LastMessageTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastMsgTimestamp))
}

// RemoteAddr returns the configured destination nsqd address
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// String returns the fully-qualified address
// 返回nsqd的网络地址
func (c *Conn) String() string {
	return c.addr
}

// Read performs a deadlined read on the underlying TCP connection
func (c *Conn) Read(p []byte) (int, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	return c.r.Read(p)
}

// Write performs a deadlined write on the underlying TCP connection
// 对底层TCP连接执行截止日期写入
func (c *Conn) Write(p []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	return c.w.Write(p)
}

// WriteCommand is a goroutine safe method to write a Command
// to this connection, and flush.
// 在这个连接上写入命令并刷新 是一个goroutine安全的方法。
func (c *Conn) WriteCommand(cmd *Command) error {
	c.mtx.Lock()

	// 发送command
	_, err := cmd.WriteTo(c)
	if err != nil {
		goto exit
	}
	// 刷新缓冲数据
	err = c.Flush()

exit:
	c.mtx.Unlock()
	if err != nil {
		c.log(LogLevelError, "IO error - %s", err)
		c.delegate.OnIOError(c, err) // 写入异常
	}
	return err
}

type flusher interface {
	Flush() error
}

// Flush writes all buffered data to the underlying TCP connection
// 将所有缓冲数据写入底层TCP连接
func (c *Conn) Flush() error {
	if f, ok := c.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

// 发送identify报文
func (c *Conn) identify() (*IdentifyResponse, error) {
	ci := make(map[string]interface{})
	ci["client_id"] = c.config.ClientID
	ci["hostname"] = c.config.Hostname
	ci["user_agent"] = c.config.UserAgent
	ci["short_id"] = c.config.ClientID // deprecated
	ci["long_id"] = c.config.Hostname  // deprecated
	ci["tls_v1"] = c.config.TlsV1
	ci["deflate"] = c.config.Deflate
	ci["deflate_level"] = c.config.DeflateLevel
	ci["snappy"] = c.config.Snappy
	ci["feature_negotiation"] = true
	if c.config.HeartbeatInterval == -1 {
		ci["heartbeat_interval"] = -1
	} else {
		ci["heartbeat_interval"] = int64(c.config.HeartbeatInterval / time.Millisecond)
	}
	ci["sample_rate"] = c.config.SampleRate
	ci["output_buffer_size"] = c.config.OutputBufferSize
	if c.config.OutputBufferTimeout == -1 {
		ci["output_buffer_timeout"] = -1
	} else {
		ci["output_buffer_timeout"] = int64(c.config.OutputBufferTimeout / time.Millisecond)
	}
	ci["msg_timeout"] = int64(c.config.MsgTimeout / time.Millisecond)
	cmd, err := Identify(ci)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	// 写入identify请求
	err = c.WriteCommand(cmd)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	// 读取响应
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	// 异常的响应
	if frameType == FrameTypeError {
		return nil, ErrIdentify{string(data)}
	}

	// check to see if the server was able to respond w/ capabilities
	// i.e. it was a JSON response
	// 非json格式
	if data[0] != '{' {
		return nil, nil
	}

	resp := &IdentifyResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	c.log(LogLevelDebug, "IDENTIFY response: %+v", resp)

	// 服务器支持的消息数量
	c.maxRdyCount = resp.MaxRdyCount

	if resp.TLSv1 {
		c.log(LogLevelInfo, "upgrading to TLS")
		err := c.upgradeTLS(c.config.TlsConfig)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Deflate {
		c.log(LogLevelInfo, "upgrading to Deflate")
		err := c.upgradeDeflate(c.config.DeflateLevel)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Snappy {
		c.log(LogLevelInfo, "upgrading to Snappy")
		err := c.upgradeSnappy()
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	// now that connection is bootstrapped, enable read buffering
	// (and write buffering if it's not already capable of Flush())
	// 现在连接已经启动，启用读缓冲(如果还不能使用Flush()则启用写缓冲)
	c.r = bufio.NewReader(c.r)
	if _, ok := c.w.(flusher); !ok { // 不具有缓冲功能
		c.w = bufio.NewWriter(c.w)
	}

	return resp, nil
}

// 客户端升级tls
func (c *Conn) upgradeTLS(tlsConf *tls.Config) error {
	host, _, err := net.SplitHostPort(c.addr)
	if err != nil {
		return err
	}

	// create a local copy of the config to set ServerName for this connection
	conf := &tls.Config{}
	if tlsConf != nil {
		conf = tlsConf.Clone()
	}
	conf.ServerName = host

	c.tlsConn = tls.Client(c.conn, conf)
	err = c.tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.r = c.tlsConn
	c.w = c.tlsConn
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from TLS upgrade")
	}
	return nil
}

// 客户端升级deflate压缩
func (c *Conn) upgradeDeflate(level int) error {
	conn := net.Conn(c.conn)
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	fw, _ := flate.NewWriter(conn, level)
	c.r = flate.NewReader(conn)
	c.w = fw
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Deflate upgrade")
	}
	return nil
}

// 客户端升级snappy压缩
func (c *Conn) upgradeSnappy() error {
	conn := net.Conn(c.conn)
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	c.r = snappy.NewReader(conn)
	c.w = snappy.NewWriter(conn)
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Snappy upgrade")
	}
	return nil
}

// 同nsqd进行auth认证
func (c *Conn) auth(secret string) error {
	cmd, err := Auth(secret)
	if err != nil {
		return err
	}

	// 写入auth命令
	err = c.WriteCommand(cmd)
	if err != nil {
		return err
	}

	// 读取auth认证结果
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}

	// 异常相应报文
	if frameType == FrameTypeError {
		return errors.New("Error authenticating " + string(data))
	}

	resp := &AuthResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return err
	}

	c.log(LogLevelInfo, "Auth accepted. Identity: %q %s Permissions: %d",
		resp.Identity, resp.IdentityUrl, resp.PermissionCount)

	return nil
}

// 循环读取tcp报文
func (c *Conn) readLoop() {
	delegate := &connMessageDelegate{c}
	for {
		// 链接已关闭
		if atomic.LoadInt32(&c.closeFlag) == 1 {
			goto exit
		}

		// 读取响应内容
		frameType, data, err := ReadUnpackedResponse(c)
		// 读取失败
		if err != nil {
			// 已经被关闭
			if err == io.EOF && atomic.LoadInt32(&c.closeFlag) == 1 {
				goto exit
			}

			// 读取已关闭的链接
			if !strings.Contains(err.Error(), "use of closed network connection") {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
			}
			goto exit
		}

		// 服务器端的心跳包
		if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
			c.log(LogLevelDebug, "heartbeat received")
			c.delegate.OnHeartbeat(c) // 回调
			// 写入空操作命令 校验写入是否正常
			err := c.WriteCommand(Nop())
			if err != nil {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			continue
		}

		switch frameType {
		case FrameTypeResponse: // 响应消息
			c.delegate.OnResponse(c, data)
		case FrameTypeMessage: // message消息
			msg, err := DecodeMessage(data) // 反序列化message
			if err != nil {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			msg.Delegate = delegate      // 回调函数
			msg.NSQDAddress = c.String() // nsqd网络地址

			atomic.AddInt64(&c.messagesInFlight, 1)
			atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

			c.delegate.OnMessage(c, msg)
		case FrameTypeError: // 异常响应的报文
			c.log(LogLevelError, "protocol error - %s", data)
			c.delegate.OnError(c, data)
		default:
			c.log(LogLevelError, "IO error - %s", err)
			c.delegate.OnIOError(c, fmt.Errorf("unknown frame type %d", frameType))
		}
	}

exit:
	atomic.StoreInt32(&c.readLoopRunning, 0) // 标记不再读取tcp消息
	// start the connection close
	messagesInFlight := atomic.LoadInt64(&c.messagesInFlight)
	if messagesInFlight == 0 {
		// if we exited readLoop with no messages in flight
		// we need to explicitly trigger the close because
		// writeLoop won't
		// 如果我们在没有消息的情况下退出了readLoop，我们需要显式地触发关闭，因为writeLoop不会
		c.close()
	} else {
		c.log(LogLevelWarning, "delaying close, %d outstanding messages", messagesInFlight)
	}
	c.wg.Done()
	c.log(LogLevelInfo, "readLoop exiting")
}

func (c *Conn) writeLoop() {
	for {
		select {
		case <-c.exitChan:
			c.log(LogLevelInfo, "breaking out of writeLoop")
			// Indicate drainReady because we will not pull any more off msgResponseChan
			close(c.drainReady)
			goto exit
		case cmd := <-c.cmdChan:
			err := c.WriteCommand(cmd)
			if err != nil {
				c.log(LogLevelError, "error sending command %s - %s", cmd, err)
				c.close()
				continue
			}
		case resp := <-c.msgResponseChan:
			// Decrement this here so it is correct even if we can't respond to nsqd
			msgsInFlight := atomic.AddInt64(&c.messagesInFlight, -1)

			if resp.success {
				c.log(LogLevelDebug, "FIN %s", resp.msg.ID)
				c.delegate.OnMessageFinished(c, resp.msg)
				c.delegate.OnResume(c) // 消息发送成功 关闭回退
			} else {
				c.log(LogLevelDebug, "REQ %s", resp.msg.ID)
				c.delegate.OnMessageRequeued(c, resp.msg) // 消费失败，重新写入队列
				if resp.backoff {
					c.delegate.OnBackoff(c)
				} else {
					c.delegate.OnContinue(c)
				}
			}

			err := c.WriteCommand(resp.cmd)
			if err != nil {
				c.log(LogLevelError, "error sending command %s - %s", resp.cmd, err)
				c.close()
				continue
			}

			if msgsInFlight == 0 &&
				atomic.LoadInt32(&c.closeFlag) == 1 {
				c.close()
				continue
			}
		}
	}

exit:
	c.wg.Done()
	c.log(LogLevelInfo, "writeLoop exiting")
}

func (c *Conn) close() {
	// a "clean" connection close is orchestrated as follows:
	//
	//     1. CLOSE cmd sent to nsqd
	//     2. CLOSE_WAIT response received from nsqd
	//     3. set c.closeFlag
	//     4. readLoop() exits
	//         a. if messages-in-flight > 0 delay close()
	//             i. writeLoop() continues receiving on c.msgResponseChan chan
	//                 x. when messages-in-flight == 0 call close()
	//         b. else call close() immediately
	//     5. c.exitChan close
	//         a. writeLoop() exits
	//             i. c.drainReady close
	//     6a. launch cleanup() goroutine (we're racing with intraprocess
	//        routed messages, see comments below)
	//         a. wait on c.drainReady
	//         b. loop and receive on c.msgResponseChan chan
	//            until messages-in-flight == 0
	//            i. ensure that readLoop has exited
	//     6b. launch waitForCleanup() goroutine
	//         b. wait on waitgroup (covers readLoop() and writeLoop()
	//            and cleanup goroutine)
	//         c. underlying TCP connection close
	//         d. trigger Delegate OnClose()
	//
	c.stopper.Do(func() {
		c.log(LogLevelInfo, "beginning close")
		close(c.exitChan)
		c.conn.CloseRead()

		c.wg.Add(1)
		go c.cleanup()

		go c.waitForCleanup()
	})
}

func (c *Conn) cleanup() {
	<-c.drainReady
	ticker := time.NewTicker(100 * time.Millisecond)
	lastWarning := time.Now()
	// writeLoop has exited, drain any remaining in flight messages
	for {
		// we're racing with readLoop which potentially has a message
		// for handling so infinitely loop until messagesInFlight == 0
		// and readLoop has exited
		var msgsInFlight int64
		select {
		case <-c.msgResponseChan:
			msgsInFlight = atomic.AddInt64(&c.messagesInFlight, -1)
		case <-ticker.C:
			msgsInFlight = atomic.LoadInt64(&c.messagesInFlight)
		}
		if msgsInFlight > 0 {
			if time.Now().Sub(lastWarning) > time.Second {
				c.log(LogLevelWarning, "draining... waiting for %d messages in flight", msgsInFlight)
				lastWarning = time.Now()
			}
			continue
		}
		// until the readLoop has exited we cannot be sure that there
		// still won't be a race
		if atomic.LoadInt32(&c.readLoopRunning) == 1 {
			if time.Now().Sub(lastWarning) > time.Second {
				c.log(LogLevelWarning, "draining... readLoop still running")
				lastWarning = time.Now()
			}
			continue
		}
		goto exit
	}

exit:
	ticker.Stop()
	c.wg.Done()
	c.log(LogLevelInfo, "finished draining, cleanup exiting")
}

func (c *Conn) waitForCleanup() {
	// this blocks until readLoop and writeLoop
	// (and cleanup goroutine above) have exited
	c.wg.Wait()
	c.conn.CloseWrite()
	c.log(LogLevelInfo, "clean close complete")
	c.delegate.OnClose(c)
}

func (c *Conn) onMessageFinish(m *Message) {
	c.msgResponseChan <- &msgResponse{msg: m, cmd: Finish(m.ID), success: true}
}

// 重新写入队列
func (c *Conn) onMessageRequeue(m *Message, delay time.Duration, backoff bool) {
	if delay == -1 {
		// linear delay  线性延迟
		delay = c.config.DefaultRequeueDelay * time.Duration(m.Attempts)
		// bound the requeueDelay to configured max 绑定requeueDelay到配置的最大
		if delay > c.config.MaxRequeueDelay {
			delay = c.config.MaxRequeueDelay
		}
	}
	c.msgResponseChan <- &msgResponse{msg: m, cmd: Requeue(m.ID, delay), success: false, backoff: backoff}
}

// 重置消息超时时间
func (c *Conn) onMessageTouch(m *Message) {
	select {
	case c.cmdChan <- Touch(m.ID):
	case <-c.exitChan:
	}
}

func (c *Conn) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl, logFmt := c.getLogger(lvl)

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %s %s", lvl,
		fmt.Sprintf(logFmt, c.String()),
		fmt.Sprintf(line, args...)))
}
