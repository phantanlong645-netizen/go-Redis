package parser

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

type Payload struct {
	Data [][]byte
	Err  error
}

// set name A
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}
func parse0(rawreader io.Reader, ch chan<- *Payload) {
	defer close(ch)
	reader := bufio.NewReader(rawreader)
	for {
		data, err := readCommand(reader)
		if err != nil {
			if err != io.EOF {
				ch <- &Payload{Err: err}
			}
			return
		}
		if len(data) == 0 {
			continue
		}
		ch <- &Payload{
			Data: data,
		}
	}
}
func readLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("protocol error")
	}

	return line[:len(line)-2], nil
}
func readCommand(reader *bufio.Reader) ([][]byte, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] == '*' {
		return parseArray(reader, line[1:])
	}
	return parseInline(line), nil
}
func parseArray(reader *bufio.Reader, line []byte) ([][]byte, error) {
	count, err := strconv.Atoi(string(line))
	if err != nil || count < 0 {
		return nil, fmt.Errorf("protocol error:invalid array count")
	}
	result := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		line2, err2 := readLine(reader)
		if err2 != nil {
			return nil, err2
		}
		if len(line2) == 0 || line2[0] != '$' {
			return nil, fmt.Errorf("protocol error: expected bulk string")
		}
		size, err3 := strconv.Atoi(string(line2[1:]))
		if err3 != nil || size < 0 {
			return nil, fmt.Errorf("protocol error: invalid bulk lengt")
		}
		arg := make([]byte, size+2)
		_, err = io.ReadFull(reader, arg)
		if err != nil {
			return nil, err
		}
		if arg[size] != '\r' || arg[size+1] != '\n' {
			return nil, fmt.Errorf("protocol error: bad bulk terminator")
		}
		result = append(result, append([]byte(nil), arg[:size]...))
	}
	return result, nil
}

func parseInline(line []byte) [][]byte {
	fields := bytes.Fields(line)
	result := make([][]byte, 0, len(fields))
	for _, field := range fields {
		result = append(result, append([]byte(nil), field...))
	}
	return result
}
