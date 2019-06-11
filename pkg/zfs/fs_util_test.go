package zfs

import (
	"bufio"
	"bytes"
	"testing"
)

var procMountInfoTestData = `900 810 0:188 / / rw,relatime - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/DCWHICUHNNVZJ7P5YV4XC2RTMB:/var/lib/docker/overlay2/l/4PNMTHPRNVWYIJERQXQRTHDAX5:/var/lib/docker/overlay2/l/LLRBDQSW3TJMXKTELDWOA3VCON:/var/lib/docker/overlay2/l/T6KNL7LBQRRC5WMKIDQ6A7ROWV:/var/lib/docker/overlay2/l/FLJZO3TOPP2OPFCOXWJ5FF2O72:/var/lib/docker/overlay2/l/MJYWIJ6RJERP4IMAEKZ26LQE6P:/var/lib/docker/overlay2/l/4W2FF325POJBE5P226KJIRB7VB:/var/lib/docker/overlay2/l/AKEWQBDFPNKGGIA4FPA6CTAKJG,upperdir=/var/lib/docker/overlay2/16689a9da2a141620c5956d94750cf2c4ca15fbabb4d97952fed441360d63499/diff,workdir=/var/lib/docker/overlay2/16689a9da2a141620c5956d94750cf2c4ca15fbabb4d97952fed441360d63499/work
901 900 0:4 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
902 900 0:199 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
903 902 0:200 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
1000 900 0:201 / /sys rw,nosuid,nodev,noexec,relatime - sysfs sysfs rw
1001 1000 0:202 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755
1002 1001 0:28 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/systemd rw,nosuid,nodev,noexec,relatime shared:11 - cgroup cgroup rw,xattr,name=systemd
1003 1001 0:30 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/freezer rw,nosuid,nodev,noexec,relatime shared:14 - cgroup cgroup rw,freezer
1004 1001 0:31 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/hugetlb rw,nosuid,nodev,noexec,relatime shared:15 - cgroup cgroup rw,hugetlb
1005 1001 0:32 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/perf_event rw,nosuid,nodev,noexec,relatime shared:16 - cgroup cgroup rw,perf_event
1006 1001 0:33 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/net_cls,net_prio rw,nosuid,nodev,noexec,relatime shared:17 - cgroup cgroup rw,net_cls,net_prio
1007 1001 0:34 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/blkio rw,nosuid,nodev,noexec,relatime shared:18 - cgroup cgroup rw,blkio
1008 1001 0:35 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime shared:19 - cgroup cgroup rw,cpu,cpuacct
1009 1001 0:36 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/devices rw,nosuid,nodev,noexec,relatime shared:20 - cgroup cgroup rw,devices
1010 1001 0:37 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/pids rw,nosuid,nodev,noexec,relatime shared:21 - cgroup cgroup rw,pids
1011 1001 0:38 / /sys/fs/cgroup/rdma rw,nosuid,nodev,noexec,relatime shared:22 - cgroup cgroup rw,rdma
1012 1001 0:39 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:23 - cgroup cgroup rw,memory
1013 1001 0:40 /docker/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565 /sys/fs/cgroup/cpuset rw,nosuid,nodev,noexec,relatime shared:24 - cgroup cgroup rw,cpuset
1014 902 0:198 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
1015 900 8:1 /var/lib/docker/volumes/dotmesh-boltdb/_data /data rw,relatime shared:1 - ext4 /dev/sda1 rw,data=ordered
1016 900 8:1 /usr/libexec/kubernetes/kubelet-plugins/volume/exec /system-flexvolume rw,relatime - ext4 /dev/sda1 rw,data=ordered
1017 900 8:1 /var/lib/docker/volumes/dotmesh-kernel-modules/_data /bundled-lib rw,relatime shared:1 - ext4 /dev/sda1 rw,data=ordered
1018 902 0:197 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=1048576k
1019 900 8:1 /var/lib/docker/containers/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565/resolv.conf /etc/resolv.conf rw,relatime - ext4 /dev/sda1 rw,data=ordered
1020 900 8:1 /var/lib/docker/containers/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565/hostname /etc/hostname rw,relatime - ext4 /dev/sda1 rw,data=ordered
1021 900 8:1 /var/lib/docker/containers/9dc9271dc21015e3a3edc26c9a6985459a39050713269167dda49301cfdec565/hosts /etc/hosts rw,relatime - ext4 /dev/sda1 rw,data=ordered
1022 900 0:23 /docker.sock /run/docker.sock rw,nosuid,noexec,relatime - tmpfs tmpfs rw,size=3087560k,mode=755
1023 900 0:23 /docker/plugins /run/docker/plugins rw,nosuid,noexec,relatime - tmpfs tmpfs rw,size=3087560k,mode=755
1024 900 8:1 /var/lib/dotmesh /var/lib/dotmesh rw,relatime shared:1 - ext4 /dev/sda1 rw,data=ordered
811 1024 0:203 / /var/lib/dotmesh/mnt/dmfs/0a5bb16f-0e7c-456b-9487-823634d09f13 rw,noatime shared:334 - zfs pool/dmfs/0a5bb16f-0e7c-456b-9487-823634d09f13 rw,xattr,noacl
866 1024 0:204 / /var/lib/dotmesh/mnt/dmfs/8709de2a-f4c0-4d38-9241-61ca16c6764f rw,noatime shared:343 - zfs pool/dmfs/8709de2a-f4c0-4d38-9241-61ca16c6764f rw,xattr,noacl
889 1024 0:205 / /var/lib/dotmesh/mnt/dmfs/fa976a3f-8d8b-4da2-a500-917cca5e79f8 rw,noatime shared:352 - zfs pool/dmfs/fa976a3f-8d8b-4da2-a500-917cca5e79f8 rw,xattr,noacl
1048 1024 0:206 / /var/lib/dotmesh/mnt/dmfs/b32ce0a5-e902-4dfe-8050-2f4e36ce0eca rw,noatime shared:361 - zfs pool/dmfs/b32ce0a5-e902-4dfe-8050-2f4e36ce0eca rw,xattr,noacl`

func TestMountpointFilter(t *testing.T) {
	buf := bytes.NewBufferString(procMountInfoTestData)

	reader := bufio.NewReader(buf)

	filtered, err := filterMountpoints("/var/lib/dotmesh/mnt", "8709de2a-f4c0-4d38-9241-61ca16c6764f", reader)
	if err != nil {
		t.Errorf("failed to filter: %s", err)
	}
	if len(filtered) != 1 {
		t.Errorf("expected 1 entry, got: %d", len(filtered))
	}
	t.Log(filtered)
}

var procMountInfoTestDataWithSnapshots = `4761 22 0:167 / /var/lib/docker/containers/d3be9bb3305b151f230d3b117650686621888adf40fb008f7d78452fae9e79de/mounts/shm rw,nosuid,nodev,noexec,relatime shared:550 - tmpfs shm rw,size=65536k
206 29 0:3 net:[4026533394] /run/docker/netns/146f70b42e20 rw shared:1558 - nsfs nsfs rw
88 22 0:97 / /var/lib/docker/overlay2/ea5ed091b17dd647581bbc7543923174fe467eb26456b264211513838f7b59c5/merged rw,relatime shared:85 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/BZXFAXJFMGHEUOVKUCY25W27YN:/var/lib/docker/overlay2/l/YWZNJVAQQAWBNGXLWXOI5S4K4M:/var/lib/docker/overlay2/l/3GWAFBEWHPAAAOKNA3FLXYIS75:/var/lib/docker/overlay2/l/IWDTCI2G3EJ2YJNPQWIPJCSVEV:/var/lib/docker/overlay2/l/R5OT4FQ42MGCFI4DLSDXT6CIOZ:/var/lib/docker/overlay2/l/HMAMPU53HP353HISEIWQV7HCXR:/var/lib/docker/overlay2/l/I7XFZ7LAOJUV44HUEYVLWOIVYX:/var/lib/docker/overlay2/l/RTO36FZCXP2ILJBK6YAGL54DRC:/var/lib/docker/overlay2/l/SYAGX7XNICEJBOXOHSBEZX4OTR:/var/lib/docker/overlay2/l/UZGIS3UTNFEFIAYHTK7DHJZ752,upperdir=/var/lib/docker/overlay2/ea5ed091b17dd647581bbc7543923174fe467eb26456b264211513838f7b59c5/diff,workdir=/var/lib/docker/overlay2/ea5ed091b17dd647581bbc7543923174fe467eb26456b264211513838f7b59c5/work
569 22 0:234 / /var/lib/docker/containers/4b68cc32a9b195e21fdec009c4b707cc6c9a6d36f1e961ca5c5c74e6bd2b96ec/mounts/shm rw,nosuid,nodev,noexec,relatime shared:120 - tmpfs shm rw,size=65536k
913 22 0:461 / /var/lib/dotmesh/mnt/dmfs/6231f88a-630c-4d8d-8749-647020ea1391 rw,noatime shared:1566 - zfs pool/dmfs/6231f88a-630c-4d8d-8749-647020ea1391 rw,xattr,noacl
1611 22 0:565 / /var/lib/dotmesh/mnt/dmfs/6231f88a-630c-4d8d-8749-647020ea1391@a5e72133-6521-4724-825c-c7099f1012f4 ro,noatime shared:1570 - zfs pool/dmfs/6231f88a-630c-4d8d-8749-647020ea1391@a5e72133-6521-4724-825c-c7099f1012f4 ro,xattr,noacl
2042 22 0:566 / /var/lib/dotmesh/mnt/dmfs/6231f88a-630c-4d8d-8749-647020ea1391@8d3fa57d-d2c6-4886-9954-d1b504f9b171 ro,noatime shared:1863 - zfs pool/dmfs/6231f88a-630c-4d8d-8749-647020ea1391@8d3fa57d-d2c6-4886-9954-d1b504f9b171 ro,xattr,noacl
2466 22 0:639 / /var/lib/dotmesh/mnt/dmfs/6231f88a-630c-4d8d-8749-647020ea1391@5925f87f-e589-4081-861c-cced95ab8316 ro,noatime shared:1867 - zfs pool/dmfs/6231f88a-630c-4d8d-8749-647020ea1391@5925f87f-e589-4081-861c-cced95ab8316 ro,xattr,noacl
3457 22 0:640 / /var/lib/dotmesh/mnt/dmfs/6231f88a-630c-4d8d-8749-647020ea1391@5f3b3f90-50b3-441a-9e8b-b90b780514a2 ro,noatime shared:1871 - zfs pool/dmfs/6231f88a-630c-4d8d-8749-647020ea1391@5f3b3f90-50b3-441a-9e8b-b90b780514a2 ro,xattr,noacl
3521 22 0:645 / /var/lib/dotmesh/mnt/dmfs/de49b8d1-f88b-4c87-8ad4-eca8e9f157b5 rw,noatime shared:1875 - zfs pool/dmfs/de49b8d1-f88b-4c87-8ad4-eca8e9f157b5 rw,xattr,noacl
3667 22 0:646 / /var/lib/dotmesh/mnt/dmfs/de49b8d1-f88b-4c87-8ad4-eca8e9f157b5@5f3b3f90-50b3-441a-9e8b-b90b780514a2 ro,noatime shared:1879 - zfs pool/dmfs/de49b8d1-f88b-4c87-8ad4-eca8e9f157b5@5f3b3f90-50b3-441a-9e8b-b90b780514a2 ro,xattr,noacl
3767 22 0:647 / /var/lib/dotmesh/mnt/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3 rw,noatime shared:1883 - zfs pool/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3 rw,xattr,noacl
3795 22 0:648 / /var/lib/dotmesh/mnt/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3@45156382-b873-41c9-91ee-7588481452a9 ro,noatime shared:1887 - zfs pool/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3@45156382-b873-41c9-91ee-7588481452a9 ro,xattr,noacl
3808 22 0:649 / /var/lib/dotmesh/mnt/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3@2a347076-edee-4030-82b3-c22b2a6d0100 ro,noatime shared:1891 - zfs pool/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3@2a347076-edee-4030-82b3-c22b2a6d0100 ro,xattr,noacl
3835 22 0:650 / /var/lib/dotmesh/mnt/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3@1461c8df-674a-4842-a0af-e9b370299e4f ro,noatime shared:1895 - zfs pool/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3@1461c8df-674a-4842-a0af-e9b370299e4f ro,xattr,noacl`

func TestMountpointFilterWithSnapshots(t *testing.T) {
	buf := bytes.NewBufferString(procMountInfoTestDataWithSnapshots)

	reader := bufio.NewReader(buf)

	filtered, err := filterMountpoints("/var/lib/dotmesh/mnt", "5887919c-c980-4d5f-983d-edd0f0d76be3", reader)
	if err != nil {
		t.Errorf("failed to filter: %s", err)
	}
	if len(filtered) != 4 {
		t.Errorf("expected 4 entry, got: %d", len(filtered))
	}
	if filtered[0] != "/var/lib/dotmesh/mnt/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3@1461c8df-674a-4842-a0af-e9b370299e4f" {
		t.Errorf("unexpected first entry")
	}
	if filtered[3] != "/var/lib/dotmesh/mnt/dmfs/5887919c-c980-4d5f-983d-edd0f0d76be3" {
		t.Errorf("expected last entry to be without snapshot, got: %s", filtered[3])
	}
	t.Log(filtered)
}
