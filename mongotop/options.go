package mongotop

var Usage = `<options> <polling interval in seconds>

Monitor basic usage statistics for each collection.

See http://docs.mongodb.org/manual/reference/program/mongotop/ for more information.`

// Output defines the set of options to use in displaying data from the server.
type Output struct {
	Locks     bool `long:"locks" description:"report on use of per-database locks"`
	RowCount  int  `long:"rowcount" value-name:"<count>" short:"n" description:"number of stats lines to print (0 for indefinite)"`
	ListCount int  `long:"listcount" value-name:"<count>" short:"l" description:"number of entry lines to print per stat row (0 defaults to 10)"`
	SortLatency bool  `long:"sortlatency" short:"s" description:"sort entries by average total ms / op instead of default of total time"`
	Json      bool `long:"json" description:"format output as JSON"`
}

// Name returns a human-readable group name for output options.
func (_ *Output) Name() string {
	return "output"
}
