# Basic usage

```go
/* launch norikra before testing*/

c := norikra.New("localhost", 26571)
c.Open("target1", nil, true)
defer c.Close("target1")
c.Register("query1", "", "select data from target1 where id > 1")

events := []interface{}{
	map[string]interface{}{
		"id":   3,
		"data": "a",
	},
	map[string]interface{}{
		"id":   2,
		"data": "b",
	},
	map[string]interface{}{
		"id":   1,
		"data": "c",
	},
}
err := c.Send("target1", events)
if err != nil {
	log.Printf("err: %s", err)
}

results, err := c.See("query1")
for _, e := range results {
	data := string(e["data"].([]uint8))
	log.Printf("data: %s", data)
}
```
