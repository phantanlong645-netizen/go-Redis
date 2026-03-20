package parser

import (
	"bufio"
	"bytes"
	"io"
)

type Payload struct {
	Data [][]byte
	Err  error
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}
func parse0(rawreader io.Reader, ch chan<- *Payload) {
	reader := bufio.NewReader(rawreader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		parts := bytes.Split(line, []byte{' '})
		ch <- &Payload{
			Data: parts,
		}
	}
}
