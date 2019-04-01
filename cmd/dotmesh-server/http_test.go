package main

import (
	"net/http"
	_ "net/http/pprof"
	"testing"
)

func MustNewRequest(method, url string) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		panic(err)
	}
	return req
}

func Test_sanitizeURL(t *testing.T) {
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "simple",
			args: args{MustNewRequest("GET", "http://example.com/bar")},
			want: "/bar",
		},
		{
			name: "uuid",
			args: args{MustNewRequest("GET", "/filesystems/1c77c415-f2b4-492c-adf9-330c860b2664/START/b45da2d6-10f5-4716-b2f3-8abc9a9dad68")},
			want: "/filesystems/*/START/*",
		},
		{
			name: "token in the URL",
			args: args{MustNewRequest("GET", "/filesystems/1c77c415-f2b4-492c-adf9-330c860b2664/START/b45da2d6-10f5-4716-b2f3-8abc9a9dad68?token=verysecret")},
			want: "/filesystems/*/START/*",
		},
		{
			name: "trim s3 files",
			args: args{MustNewRequest("GET", "/s3/lukengctest:project-d9ec0bc1-default-workspace")},
			want: "/s3/*",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeURL(tt.args.r); got != tt.want {
				t.Errorf("sanitizeURL() = %v, want %v", got, tt.want)
			}
		})
	}
}
