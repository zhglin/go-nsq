package nsq

import (
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"time"
)

// The number of bytes for a Message.ID
const MsgIDLength = 16

// MessageID is the ASCII encoded hexadecimal message ID
type MessageID [MsgIDLength]byte

// Message is the fundamental data type containing
// the id, body, and metadata
// Message是包含id、主体和元数据的基本数据类型
type Message struct {
	ID        MessageID // 消息id
	Body      []byte    // 消息内容
	Timestamp int64     // 时间戳
	Attempts  uint16    // 接收次数

	NSQDAddress string // nsqd的网络地址

	Delegate MessageDelegate // 回调函数

	autoResponseDisabled int32 // 是否自动fin req
	responded            int32 // 是否已回复此消息 fin requeue touch
}

// NewMessage creates a Message, initializes some metadata,
// and returns a pointer
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// DisableAutoResponse disables the automatic response that
// would normally be sent when a handler.HandleMessage
// returns (FIN/REQ based on the error value returned).
//
// This is useful if you want to batch, buffer, or asynchronously
// respond to messages.
// 禁用处理程序时通常会发送的自动响应。
// HandleMessage返回(FIN/REQ基于返回的错误值)。如果您想对消息进行批处理、缓冲或异步响应，这将非常有用。
func (m *Message) DisableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 1)
}

// IsAutoResponseDisabled indicates whether or not this message
// will be responded to automatically
func (m *Message) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}

// HasResponded indicates whether or not this message has been responded to
// 指示是否已响应此消息
func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// Finish sends a FIN command to the nsqd which
// sent this message
// 向发送此消息的nsqd发送FIN命令
func (m *Message) Finish() {
	// 是否已发送响应
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnFinish(m)
}

// Touch sends a TOUCH command to the nsqd which
// sent this message
// 向发送此消息的nsqd发送TOUCH命令
func (m *Message) Touch() {
	if m.HasResponded() {
		return
	}
	m.Delegate.OnTouch(m)
}

// Requeue sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// A delay of -1 will automatically calculate
// based on the number of attempts and the
// configured default_requeue_delay
// 使用提供的延迟向发送此消息的nsqd发送REQ命令。
// 延迟为-1将根据尝试次数和配置的default_requeue_delay自动计算
func (m *Message) Requeue(delay time.Duration) {
	m.doRequeue(delay, true)
}

// RequeueWithoutBackoff sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// Notably, using this method to respond does not trigger a backoff
// event on the configured Delegate.
// 使用提供的延迟向发送此消息的nsqd发送REQ命令。
// 值得注意的是，使用此方法响应不会触发配置的委派上的回退事件。
func (m *Message) RequeueWithoutBackoff(delay time.Duration) {
	m.doRequeue(delay, false)
}

func (m *Message) doRequeue(delay time.Duration, backoff bool) {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnRequeue(m, delay, backoff)
}

// WriteTo implements the WriterTo interface and serializes
// the message into the supplied producer.
//
// It is suggested that the target Writer is buffered to
// avoid performing many system calls.
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// DecodeMessage deserializes data (as []byte) and creates a new Message
// message format:
//  [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
//  |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
//  |       8-byte         ||    ||                 16-byte                      || N-byte
//  ------------------------------------------------------------------------------------------...
//    nanosecond timestamp    ^^                   message ID                       message body
//                         (uint16)
//                          2-byte
//                         attempts
// 反序列化数据(以[]byte)并创建新的Message消息格式
func DecodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < 10+MsgIDLength {
		return nil, errors.New("not enough data to decode valid message")
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}
