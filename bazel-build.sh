
location=$(realpath .)/bazel-bin/cmd
echo "Output path inside dazel will be $location"
./rebuild_client.sh $1
./rebuild_server.sh
./rebuild_provisioner.sh
./rebuild_flexvolume.sh
./rebuild_operator.sh
