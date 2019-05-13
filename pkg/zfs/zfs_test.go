package zfs

import (
	"reflect"
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

var testZfsOutputMinimal = `M	/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/dotscience_logs/7c029fd1-461c-447e-a447-aa36bdc4d3b3
R	/var/lib/dotmesh/mnt/dmfs/f7f660b4-6154-497a-89ff-5053c41d1ff4/__default__/foo-bar.txt -> /var/lib/dotmesh/mnt/dmfs/f7f660b4-6154-497a-89ff-5053c41d1ff4/__default__/foo-bar-2.txt
+	/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/__default__/dest
-	/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/__default__/dest/train_images.zip
+	/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/__default__/dest/test.csv`

func Test_parseZFSDiffOutput(t *testing.T) {
	type args struct {
		data string
	}
	tests := []struct {
		name string
		args args
		want []types.ZFSFileDiff
	}{
		{
			name: "minimal parse",
			args: args{data: testZfsOutputMinimal},
			want: []types.ZFSFileDiff{
				{
					Change:   types.FileChangeModified,
					Filename: "/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/dotscience_logs/7c029fd1-461c-447e-a447-aa36bdc4d3b3",
				},
				{
					Change:   types.FileChangeRenamed,
					Filename: "/var/lib/dotmesh/mnt/dmfs/f7f660b4-6154-497a-89ff-5053c41d1ff4/__default__/foo-bar.txt -> /var/lib/dotmesh/mnt/dmfs/f7f660b4-6154-497a-89ff-5053c41d1ff4/__default__/foo-bar-2.txt",
				},
				{
					Change:   types.FileChangeAdded,
					Filename: "/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/__default__/dest",
				},
				{
					Change:   types.FileChangeRemoved,
					Filename: "/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/__default__/dest/train_images.zip",
				},
				{
					Change:   types.FileChangeAdded,
					Filename: "/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/__default__/dest/test.csv",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseZFSDiffOutput(tt.args.data)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseZFSDiffOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_filterZFSDiff(t *testing.T) {
	type args struct {
		files                []types.ZFSFileDiff
		snapshotOrFilesystem string
	}
	tests := []struct {
		name string
		args args
		want []types.ZFSFileDiff
	}{
		{
			name: "single in",
			args: args{
				files: []types.ZFSFileDiff{
					{
						Change:   types.FileChangeAdded,
						Filename: "/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/__default__/dest/test.csv",
					},
				},
				snapshotOrFilesystem: "fb042d52-7d76-450a-8c5b-58704ee9477f",
			},
			want: []types.ZFSFileDiff{
				{
					Change:   types.FileChangeAdded,
					Filename: "dest/test.csv",
				},
			},
		},
		{
			name: "single out",
			args: args{
				files: []types.ZFSFileDiff{
					{
						Change:   types.FileChangeAdded,
						Filename: "/var/lib/dotmesh/mnt/dmfs/fb042d52-7d76-450a-8c5b-58704ee9477f/dotscience_logs/32460130-9407-4ea9-8e2d-018c86c07cec/workload-stdout.log",
					},
				},
				snapshotOrFilesystem: "fb042d52-7d76-450a-8c5b-58704ee9477f",
			},
			want: []types.ZFSFileDiff{},
		},
		{
			name: "renamed",
			args: args{
				files: []types.ZFSFileDiff{
					{
						Change:   types.FileChangeRenamed,
						Filename: "/var/lib/dotmesh/mnt/dmfs/f7f660b4-6154-497a-89ff-5053c41d1ff4/__default__/foo-bar.txt -> /var/lib/dotmesh/mnt/dmfs/f7f660b4-6154-497a-89ff-5053c41d1ff4/__default__/foo-bar-2.txt",
					},
				},
				snapshotOrFilesystem: "f7f660b4-6154-497a-89ff-5053c41d1ff4",
			},
			want: []types.ZFSFileDiff{
				{
					Change:   types.FileChangeRenamed,
					Filename: "foo-bar.txt -> foo-bar-2.txt",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterZFSDiff(tt.args.files, tt.args.snapshotOrFilesystem); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterZFSDiff() = %v, want %v", got, tt.want)
			}
		})
	}
}
