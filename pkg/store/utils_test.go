package store

import "testing"

func Test_extractIDs(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   string
		wantErr bool
	}{
		{
			name: "simple",
			args: args{
				key: RegistryFilesystemsPrefix + "namespacex" + "/" + "fnx",
			},
			want:    "namespacex",
			want1:   "fnx",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := extractIDs(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("extractIDs() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("extractIDs() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
