Basic usage:

```
/* launch norikra */
c := New("localhost", 26571)
c.Open("target1", nil, true)
c.Register("query1", "", "select data from target1 where id=3")

events := []interface{}{
	map[string]string{
		"test": "a",
	},
	map[string]string{
		"test": "b",
	},
}
err := c.Send("target1", events)
results, err = c.See("query1")
/* c.Events("query1") to fetch events and wipe them from norikra*/
/* decoded from msgpack string values will be in []uint8 */
text := string(results[0]["test"].([]uint8))
/* results[1], results[2] .. */

```
