package commands

import (
	"reflect"
	"testing"
)

func Test_extractAddresses(t *testing.T) {
	type args struct {
		joinURL string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "actual example",
			args: args{
				joinURL: "cluster-15650676753872037-0-node-1=https://10.73.12.1:42380,cluster-15650676753872037-0-node-1=https://192.168.0.3:42380",
			},
			want: []string{"10.73.12.1", "192.168.0.3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractAddresses(tt.args.joinURL); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractAddresses() = %v, want %v", got, tt.want)
			}
		})
	}
}
