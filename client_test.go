package norikra

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func getClient() *Client {
	return New("localhost", 26571)
}

func TestOpen(t *testing.T) {
	c := getClient()
	err := c.Open("cc", nil, true)
	defer c.Close("cc")
	assert.NoError(t, err, "unable to open target")
}

func TestClose(t *testing.T) {
	c := getClient()
	c.Open("cc_1", nil, true)
	err := c.Close("cc_1")
	assert.NoError(t, err, "unable to close target", err)
}

func TestTargets(t *testing.T) {
	c := getClient()
	c.Open("cc_2", nil, true)
	c.Open("cc_3", nil, true)
	defer c.Close("cc_2")
	defer c.Close("cc_3")
	list, err := c.Targets()
	assert.NoError(t, err, "unable to list target", err)
	assert.Equal(t, list[0].Name, "cc_2")
	assert.Equal(t, list[1].Name, "cc_3")
}

func TestSend(t *testing.T) {
	c := getClient()
	c.Open("cc_4", nil, true)
	defer c.Close("cc_4")

	events := []interface{}{
		map[string]string{
			"test": "a",
		},
		map[string]string{
			"test": "b",
		},
	}
	err := c.Send("cc_4", events)
	assert.NoError(t, err)
}

func TestSee(t *testing.T) {
	c := getClient()
	c.Open("cc_5", nil, true)
	defer c.Close("cc_5")
	c.Register("q5", "", "select test from cc_5")

	events := []interface{}{
		map[string]string{
			"test": "a",
		},
		map[string]string{
			"test": "b",
		},
	}
	c.Send("cc_5", events)
	results, err := c.See("q5")
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, len(results), 2)
	text := results[0]["test"]
	assert.Equal(t, string(text.([]uint8)), "a")

	text = results[1]["test"]
	assert.Equal(t, string(text.([]uint8)), "b")

	/* twice */
	results, err = c.See("q5")
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, len(results), 2)
}

func TestRegister(t *testing.T) {
	c := getClient()
	err := c.Register("q6", "", "select a from cc_6")
	defer c.Close("cc_6")
	assert.NoError(t, err)
}

func TestQueries(t *testing.T) {
	c := getClient()
	c.Register("q7", "", "select a from cc_7")
	defer c.Close("cc_7")
	q, err := c.Queries()
	assert.NoError(t, err)
	assert.NotNil(t, q)
	if len(q) < 1 {
		t.Error("got no queries")
		return
	}
	assert.Equal(t, q[0].Name, "q7")
	assert.Equal(t, q[0].Expression, "select a from cc_7")
	assert.Equal(t, len(q[0].Targets), 1)
	assert.Equal(t, q[0].Targets[0], "cc_7")
}

func TestDeregister(t *testing.T) {
	c := getClient()
	c.Register("q8", "", "select a from cc_8")
	defer c.Close("cc_8")
	err := c.Deregister("q8")
	assert.NoError(t, err)

	q, err := c.Queries()
	assert.NoError(t, err)
	assert.NotNil(t, q)
	assert.Equal(t, len(q), 0)
}

func TestEvents(t *testing.T) {
	c := getClient()
	c.Register("q9", "", "select test from cc_9")
	defer c.Close("cc_9")

	events := []interface{}{
		map[string]string{
			"test": "a",
		},
		map[string]string{
			"test": "b",
		},
	}
	c.Send("cc_9", events)
	results, err := c.Events("q9")
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, len(results), 2)
	text := results[0]["test"]
	assert.Equal(t, string(text.([]uint8)), "a")

	text = results[1]["test"]
	assert.Equal(t, string(text.([]uint8)), "b")

	/* FIXME check content*/

	/* twice */
	results, err = c.Events("q9")
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, len(results), 0)

}
