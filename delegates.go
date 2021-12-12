package nsq

import "time"

type logger interface {
	Output(calldepth int, s string) error
}

// LogLevel specifies the severity of a given log message
type LogLevel int

// Log levels
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
	LogLevelMax = iota - 1 // convenience - match highest log level
)

// String returns the string form for a given LogLevel
func (lvl LogLevel) String() string {
	switch lvl {
	case LogLevelInfo:
		return "INF"
	case LogLevelWarning:
		return "WRN"
	case LogLevelError:
		return "ERR"
	}
	return "DBG"
}

// MessageDelegate is an interface of methods that are used as
// callbacks in Message
// 是Message中用作回调的方法的接口
type MessageDelegate interface {
	// OnFinish is called when the Finish() method
	// is triggered on the Message
	// 在Message上触发Finish()方法时调用
	OnFinish(*Message)

	// OnRequeue is called when the Requeue() method
	// is triggered on the Message
	// 在Message上触发Requeue()方法时调用
	OnRequeue(m *Message, delay time.Duration, backoff bool)

	// OnTouch is called when the Touch() method
	// is triggered on the Message
	// 在Message上触发Touch()方法时调用
	OnTouch(*Message)
}

type connMessageDelegate struct {
	c *Conn
}

func (d *connMessageDelegate) OnFinish(m *Message) { d.c.onMessageFinish(m) }
func (d *connMessageDelegate) OnRequeue(m *Message, t time.Duration, b bool) {
	d.c.onMessageRequeue(m, t, b)
}
func (d *connMessageDelegate) OnTouch(m *Message) { d.c.onMessageTouch(m) }

// ConnDelegate is an interface of methods that are used as
// callbacks in Conn
// ConnDelegate是在Conn中用作回调的方法的接口
type ConnDelegate interface {
	// OnResponse is called when the connection
	// receives a FrameTypeResponse from nsqd
	// 当连接从nsqd接收到一个FrameTypeResponse消息时调用
	OnResponse(*Conn, []byte)

	// OnError is called when the connection
	// receives a FrameTypeError from nsqd
	// 当连接从nsqd接收到一个FrameTypeError消息时调用
	OnError(*Conn, []byte)

	// OnMessage is called when the connection
	// receives a FrameTypeMessage from nsqd
	// 当连接从nsqd接收到一个FrameTypeMessage消息时调用
	OnMessage(*Conn, *Message)

	// OnMessageFinished is called when the connection
	// handles a FIN command from a message handler
	// 当连接处理来自消息处理程序的FIN命令时调用(消息确认)
	OnMessageFinished(*Conn, *Message)

	// OnMessageRequeued is called when the connection
	// handles a REQ command from a message handler
	// 当连接处理来自消息处理程序的REQ命令时调用(消息消费失败，重新写入队列)
	OnMessageRequeued(*Conn, *Message)

	// OnBackoff is called when the connection triggers a backoff state
	OnBackoff(*Conn)

	// OnContinue is called when the connection finishes a message without adjusting backoff state
	OnContinue(*Conn)

	// OnResume is called when the connection triggers a resume state
	// 消息响应发送成功的回调
	OnResume(*Conn)

	// OnIOError is called when the connection experiences
	// a low-level TCP transport error
	// 当连接遇到TCP传输错误时调用
	OnIOError(*Conn, error)

	// OnHeartbeat is called when the connection
	// receives a heartbeat from nsqd
	// 当连接从NSQD接收到心跳时调用
	OnHeartbeat(*Conn)

	// OnClose is called when the connection
	// closes, after all cleanup
	OnClose(*Conn)
}

// keeps the exported Consumer struct clean of the exported methods
// required to implement the ConnDelegate interface
type consumerConnDelegate struct {
	r *Consumer
}

func (d *consumerConnDelegate) OnResponse(c *Conn, data []byte) { d.r.onConnResponse(c, data) }
func (d *consumerConnDelegate) OnError(c *Conn, data []byte)    { d.r.onConnError(c, data) }
func (d *consumerConnDelegate) OnMessage(c *Conn, m *Message)   { d.r.onConnMessage(c, m) }
func (d *consumerConnDelegate) OnMessageFinished(c *Conn, m *Message) {
	d.r.onConnMessageFinished(c, m)
}
func (d *consumerConnDelegate) OnMessageRequeued(c *Conn, m *Message) {
	d.r.onConnMessageRequeued(c, m)
}
func (d *consumerConnDelegate) OnBackoff(c *Conn)            { d.r.onConnBackoff(c) }
func (d *consumerConnDelegate) OnContinue(c *Conn)           { d.r.onConnContinue(c) }
func (d *consumerConnDelegate) OnResume(c *Conn)             { d.r.onConnResume(c) }
func (d *consumerConnDelegate) OnIOError(c *Conn, err error) { d.r.onConnIOError(c, err) }
func (d *consumerConnDelegate) OnHeartbeat(c *Conn)          { d.r.onConnHeartbeat(c) }
func (d *consumerConnDelegate) OnClose(c *Conn)              { d.r.onConnClose(c) }

// keeps the exported Producer struct clean of the exported methods
// required to implement the ConnDelegate interface
type producerConnDelegate struct {
	w *Producer
}

func (d *producerConnDelegate) OnResponse(c *Conn, data []byte)       { d.w.onConnResponse(c, data) }
func (d *producerConnDelegate) OnError(c *Conn, data []byte)          { d.w.onConnError(c, data) }
func (d *producerConnDelegate) OnMessage(c *Conn, m *Message)         {}
func (d *producerConnDelegate) OnMessageFinished(c *Conn, m *Message) {}
func (d *producerConnDelegate) OnMessageRequeued(c *Conn, m *Message) {}
func (d *producerConnDelegate) OnBackoff(c *Conn)                     {}
func (d *producerConnDelegate) OnContinue(c *Conn)                    {}
func (d *producerConnDelegate) OnResume(c *Conn)                      {}
func (d *producerConnDelegate) OnIOError(c *Conn, err error)          { d.w.onConnIOError(c, err) }
func (d *producerConnDelegate) OnHeartbeat(c *Conn)                   { d.w.onConnHeartbeat(c) }
func (d *producerConnDelegate) OnClose(c *Conn)                       { d.w.onConnClose(c) }
