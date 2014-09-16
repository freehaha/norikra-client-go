package norikra

import (
	"errors"
	"fmt"
	"github.com/freehaha/msgpack-http-rpc"
	"log"
)

type Client struct {
	URL    string
	client *rpc.Client
}

type Target struct {
	Name      string `codec:"name"`
	AutoField bool   `codec:"auto_field"`
}

type Query struct {
	Targets    []string
	Group      string
	Name       string
	Expression string
}

type Event map[string]interface{}

func New(host string, port int) *Client {
	client := new(Client)
	client.URL = fmt.Sprintf("http://%s:%d", host, port)
	client.client = rpc.New(client.URL)
	return client
}

func (c *Client) Open(target string, fields map[string]string, autoField bool) error {
	result, err := c.client.Call("open", []interface{}{
		target,
		fields,
		autoField,
	})
	if err != nil {
		log.Printf("error: %s", err)
		return err
	}
	success := result.(bool)
	if !success {
		return errors.New("not created")
	}
	return nil
}

func (c *Client) Close(target string) error {
	result, err := c.client.Call("close", []interface{}{
		target,
	})
	if err != nil {
		log.Printf("error: %s", err)
		return err
	}
	success := result.(bool)
	if !success {
		return errors.New("not closed")
	}
	return nil
}

func (c *Client) Targets() ([]Target, error) {
	result, err := c.client.Call("targets", nil)
	if err != nil {
		log.Printf("error: %s", err)
		return nil, err
	}
	targets := make([]Target, 0, 5)

	list, ok := result.([]interface{})
	if !ok {
		return nil, errors.New("failed to get targets")
	}
	for _, t := range list {
		m := t.(map[interface{}]interface{})
		target := Target{
			Name:      string(m["name"].([]uint8)),
			AutoField: m["auto_field"].(bool),
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func (c *Client) Send(target string, events []interface{}) error {
	_, err := c.client.Call("send", []interface{}{target, events})
	if err != nil {
		log.Printf("error: %s", err)
		return err
	}
	return nil
}

func convertKeys(obj map[interface{}]interface{}) map[string]interface{} {
	var ev map[string]interface{} = make(map[string]interface{})
	for k, v := range obj {
		strKey := k.(string)
		switch t := v.(type) {
		case map[interface{}]interface{}:
			ev[strKey] = convertKeys(t)
		case []uint8:
			ev[strKey] = string(t)
		default:
			ev[strKey] = v
		}
	}
	return ev
}

func parseEvents(events []interface{}) []Event {
	var result []Event = make([]Event, 0, 10)
	for _, e := range events {
		body := e.([]interface{})[1].(map[interface{}]interface{})
		ev := convertKeys(body)
		result = append(result, ev)
	}
	return result
}

func (c *Client) See(query string) (events []Event, err error) {
	result, err := c.client.Call("see", []string{query})
	if err != nil {
		log.Printf("error: %s", err)
		return nil, err
	}

	list, ok := result.([]interface{})
	if !ok {
		return nil, errors.New("failed to get events")
	}
	events = parseEvents(list)
	return
}

func (c *Client) Events(query string) (events []Event, err error) {
	result, err := c.client.Call("event", []string{query})
	if err != nil {
		log.Printf("error: %s", err)
		return nil, err
	}

	list, ok := result.([]interface{})
	if !ok {
		return nil, errors.New("failed to get events")
	}
	events = parseEvents(list)
	return
}

func (c *Client) Register(name string, group string, query string) (err error) {
	_, err = c.client.Call("register", []string{name, group, query})
	return
}

func (c *Client) Deregister(name string) (err error) {
	_, err = c.client.Call("deregister", []string{name})
	return
}

func (c *Client) Queries() (queries []Query, err error) {
	result, err := c.client.Call("queries", nil)
	if err != nil {
		log.Printf("error: %s", err)
		return nil, err
	}
	queries = make([]Query, 0, 5)

	list, ok := result.([]interface{})
	if !ok {
		return nil, errors.New("failed to get queries")
	}
	for _, t := range list {
		m := t.(map[interface{}]interface{})
		ts := m["targets"].([]interface{})
		tss := make([]string, 0, 5)
		for _, v := range ts {
			ustr, ok := v.([]uint8)
			if !ok {
				err = errors.New("failed to read targets of query")
				return
			}
			tss = append(tss, string(ustr))
		}
		tmp, ok := m["group"]
		var group string
		if !ok || tmp == nil {
			group = ""
		} else {
			group = string(tmp.([]uint8))
		}
		query := Query{
			Group:      group,
			Name:       string(m["name"].([]uint8)),
			Expression: string(m["expression"].([]uint8)),
			Targets:    tss,
		}
		queries = append(queries, query)
	}
	return
}
