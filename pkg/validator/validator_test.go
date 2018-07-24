package validator

import (
	"reflect"
	"testing"
)

func TestIsValidVolumeName(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			args:    args{str: ""},
			wantErr: ErrEmptyName,
		},
		{
			name:    "funny characters shouldn't be valid",
			args:    args{str: "!"},
			wantErr: ErrInvalidVolumeName,
		},
		{
			name:    "too long",
			args:    args{str: "00000000001111111111222222222233333333334444444444555555555566666"},
			wantErr: ErrInvalidVolumeName,
		},
		{
			name:    "64 chars, valid",
			args:    args{str: "0000000000111111111122222222223333333333444444444455555555556666"},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotErrs := IsValidVolumeName(tt.args.str); !reflect.DeepEqual(gotErrs, tt.wantErr) {
				t.Errorf("IsValidVolumeName() = %v, want %v", gotErrs, tt.wantErr)
			}
		})
	}
}

func TestIsValidBranchName(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			args:    args{str: ""},
			wantErr: ErrEmptyName,
		},
		{
			name:    "funny characters shouldn't be valid",
			args:    args{str: "!"},
			wantErr: ErrInvalidBranchName,
		},
		{
			name:    "too long",
			args:    args{str: "00000000001111111111222222222233333333334444444444555555555566666"},
			wantErr: ErrInvalidBranchName,
		},
		{
			name:    "64 chars, valid",
			args:    args{str: "0000000000111111111122222222223333333333444444444455555555556666"},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotErrs := IsValidBranchName(tt.args.str); !reflect.DeepEqual(gotErrs, tt.wantErr) {
				t.Errorf("IsValidBranchName() = %v, want %v", gotErrs, tt.wantErr)
			}
		})
	}
}

func TestIsValidVolumeNamespace(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{

		{
			name:    "empty",
			args:    args{str: ""},
			wantErr: ErrEmptyName,
		},
		{
			name:    "funny characters shouldn't be valid",
			args:    args{str: "!"},
			wantErr: ErrInvalidNamespaceName,
		},
		{
			name:    "too long",
			args:    args{str: "00000000001111111111222222222233333333334444444444555555555566666"},
			wantErr: ErrInvalidNamespaceName,
		},
		{
			name:    "64 chars, valid",
			args:    args{str: "0000000000111111111122222222223333333333444444444455555555556666"},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotErrs := IsValidVolumeNamespace(tt.args.str); !reflect.DeepEqual(gotErrs, tt.wantErr) {
				t.Errorf("IsValidVolumeNamespace() = %v, want %v", gotErrs, tt.wantErr)
			}
		})
	}
}

func TestIsValidSubdotName(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name:    "empty",
			args:    args{str: ""},
			wantErr: ErrEmptyName,
		},
		{
			name:    "funny characters shouldn't be valid",
			args:    args{str: "!"},
			wantErr: ErrInvalidSubdotName,
		},
		{
			name:    "too long",
			args:    args{str: "00000000001111111111222222222233333333334444444444555555555566666"},
			wantErr: ErrInvalidSubdotName,
		},
		{
			name:    "64 chars, valid",
			args:    args{str: "0000000000111111111122222222223333333333444444444455555555556666"},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotErrs := IsValidSubdotName(tt.args.str); !reflect.DeepEqual(gotErrs, tt.wantErr) {
				t.Errorf("IsValidSubdotName() = %v, want %v", gotErrs, tt.wantErr)
			}
		})
	}
}
