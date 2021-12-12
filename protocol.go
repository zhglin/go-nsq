package nsq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"regexp"
)

// MagicV1 is the initial identifier sent when connecting for V1 clients
var MagicV1 = []byte("  V1")

// MagicV2 is the initial identifier sent when connecting for V2 clients
var MagicV2 = []byte("  V2") // 读取四个字节

// frame types
const (
	FrameTypeResponse int32 = 0
	FrameTypeError    int32 = 1
	FrameTypeMessage  int32 = 2
)

var validTopicChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName checks a topic name for correctness
// 检查topic名称是否正确
func IsValidTopicName(name string) bool {
	return isValidName(name)
}

// IsValidChannelName checks a channel name for correctness
// 检查channel名称是否正确
func IsValidChannelName(name string) bool {
	return isValidName(name)
}

// 名称校验
func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}

// ReadResponse is a client-side utility function to read from the supplied Reader
// according to the NSQ protocol spec:
//
//    [x][x][x][x][x][x][x][x]...
//    |  (int32) || (binary)
//    |  4-byte  || N-byte
//    ------------------------...
//        size       data
// 是一个客户端实用函数，根据NSQ协议规范从提供的Reader中读取
func ReadResponse(r io.Reader) ([]byte, error) {
	var msgSize int32

	// message size 读取4字节的消息长度
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	// 长度异常
	if msgSize < 0 {
		return nil, fmt.Errorf("response msg size is negative: %v", msgSize)
	}
	// message binary data 读取消息内容
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// UnpackResponse is a client-side utility function that unpacks serialized data
// according to NSQ protocol spec:
//
//    [x][x][x][x][x][x][x][x]...
//    |  (int32) || (binary)
//    |  4-byte  || N-byte
//    ------------------------...
//      frame ID     data
//
// Returns a triplicate of: frame type, data ([]byte), error
// 根据NSQ协议规范对序列化数据进行解包 消息类型+消息体
func UnpackResponse(response []byte) (int32, []byte, error) {
	// 四字节的消息类型
	if len(response) < 4 {
		return -1, nil, errors.New("length of response is too small")
	}

	return int32(binary.BigEndian.Uint32(response)), response[4:], nil
}

// ReadUnpackedResponse reads and parses data from the underlying
// TCP connection according to the NSQ TCP protocol spec and
// returns the frameType, data or error
// 读取并解析消息体
func ReadUnpackedResponse(r io.Reader) (int32, []byte, error) {
	resp, err := ReadResponse(r)
	if err != nil {
		return -1, nil, err
	}
	return UnpackResponse(resp)
}
