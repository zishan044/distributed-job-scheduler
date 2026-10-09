package watcher

import "testing"

func TestWatcherOptions(t *testing.T) {
	valid := DefaultOptions()
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Options){
		func(o *Options) { o.DispatchInterval = 0 },
		func(o *Options) { o.SweepInterval = 0 },
		func(o *Options) { o.QueuedTimeout = 0 },
		func(o *Options) { o.OperationTimeout = 0 },
		func(o *Options) { o.BatchSize = 0 },
		func(o *Options) { o.BatchSize = 2001 },
	} {
		o := valid
		change(&o)
		if err := o.validate(); err == nil {
			t.Fatalf("accepted invalid options: %+v", o)
		}
	}
}
