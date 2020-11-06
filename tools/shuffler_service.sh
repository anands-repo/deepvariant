#!/bin/bash

set -e

if [ $# -eq 0 ]; then
	echo "Usage: $0 <Pattern> <Working directory> <Number of buckets> <Number of threads> <Machine List> [Mount Pattern]"
	exit
fi

IMAGE_NAME=dvorak.maas/oddjobs/flink-base
PATTERN=$1
WORKDIR=$2
NUM_BUCKETS=$3
NUM_THREADS=$4
MACHINES=$(cat $5)
MOUNT_PATTERN="$HOME/DockerStorage:/root/storage"

if [ $# -eq 6 ]; then
	MOUNT_PATTERN=$6
fi

get_dockerized_path() {
	path=$1
	source_dir=$(echo $MOUNT_PATTERN | cut -f1 -d ":")
	dest_dir=$(echo $MOUNT_PATTERN | cut -f2 -d ":")
	echo $(echo "$path" | sed "s?^$source_dir?$dest_dir?g")
}

get_service_mount_pattern() {
	source_dir=$(echo $MOUNT_PATTERN | cut -f1 -d ":")
	dest_dir=$(echo $MOUNT_PATTERN | cut -f2 -d ":")
	echo "source=$source_dir,target=$dest_dir,type=bind"
}

wait_for_service() {
	name=$1
	while true; do
		sleep 20
		
		# Check docker service status
		service_status=$(docker service ps $name | tail -1 | awk '{print $6}')
		if [ $service_status == "Complete" ]; then
			echo "Service $name completed"
			sleep 2
			break
		else
			if [ $service_status != "Running" ]; then
				echo "Service $name is in a corrupt state"
				sleep 2
				exit
			fi
		fi
		echo "Waiting for status update for service $name"
	done

	# Remove service
	docker service rm $name
}

if [ -d $WORKDIR ]; then
	echo "$WORKDIR exists. Provide a directory path that doesn't exist."
	exit
fi

# Count the number of machines
NUM_MACHINES=0
machine_list=()

for m in $MACHINES; do
	NUM_MACHINES=$((NUM_MACHINES + 1))
	machine_list+=("$m")
done

mkdir -p $WORKDIR

# First create partitions
echo "Performing partitioning"
input_pattern_list=$(get_dockerized_path "$PATTERN")
workdir=$(get_dockerized_path "$WORKDIR/partitions")
partition_service_name="partition-${RANDOM}"

printf "#!/bin/bash\n\n" > $WORKDIR/partition_script.sh
printf "set -e\n\n" >> $WORKDIR/partition_script.sh
printf "set -o pipefail\n\n" >> $WORKDIR/partition_script.sh
printf "python /opt/tools/partition_tfrecords_beam.py \\
		--input_pattern_list=\"$input_pattern_list\" \\
		--workdir=\"$workdir\" \\
		--num_buckets=$NUM_BUCKETS \\
		--direct_num_workers=$NUM_THREADS \\
		--direct_running_mode=\"multi_processing\" \\
		--streaming >& $(get_dockerized_path $WORKDIR)/partitioning.log" >> $WORKDIR/partition_script.sh

docker service create \
	-t \
	--name $partition_service_name \
	--mount $(get_service_mount_pattern $MOUNT_PATTERN) \
	--constraint "node.Hostname==${machine_list[0]}" \
	--restart-condition none \
	--entrypoint /bin/bash \
	$IMAGE_NAME \
	$(get_dockerized_path $WORKDIR/partition_script.sh)

wait_for_service $partition_service_name

echo "Distributing shuffle commands over $NUM_MACHINES machines"

# Shuffle each partition
echo "Performing shuffling"
mkdir -p $WORKDIR/shuffled
touch $WORKDIR/all_shuffle_commands.sh

for i in `seq 0 $((NUM_BUCKETS - 1))`; do
	partition_prefix=$(printf "%05d" "$i")
	input_pattern_list=$(get_dockerized_path "$WORKDIR/partitions/partition${partition_prefix}-*")
	output_pattern_name=$(printf "$WORKDIR/shuffled/shuffled_partition-%05d-" "$i")
	output_pattern_prefix=$(get_dockerized_path "$output_pattern_name")
	output_dataset_name="partition_shuffle${i}"
	shuffle_script_name=$WORKDIR/shuffle_script_${partition_prefix}.sh
	shuffle_log_name=$WORKDIR/shuffle_log_${partition_prefix}.log

	echo "Creating script for shuffling partition $i"

	printf "#!/bin/bash\n\n" > $shuffle_script_name
	printf "set -e\n\n" >> $shuffle_script_name
	printf "set -o pipefail\n\n" >> $shuffle_script_name 
	printf "python /opt/tools/shuffle_tfrecords_beam.py \\
			--input_pattern_list=\"$input_pattern_list\" \\
			--output_pattern_prefix=\"$output_pattern_prefix\" \\
			--output_dataset_name=\"$output_dataset_name\" \\
			--direct_num_workers=$NUM_THREADS \\
			--direct_running_mode=\"multi_processing\" \\
			--streaming >& $(get_dockerized_path $shuffle_log_name)" >> $shuffle_script_name
	echo "bash $(get_dockerized_path $shuffle_script_name)" >> $WORKDIR/all_shuffle_commands.sh
done

# Divide shuffle commands and launch services
num_shuffle_jobs=$(cat $WORKDIR/all_shuffle_commands.sh | wc -l)
num_jobs_per_machine=$((num_shuffle_jobs / NUM_MACHINES))

if [ $((num_jobs_per_machine * NUM_MACHINES)) -lt $num_shuffle_jobs ]; then
	num_jobs_per_machine=$((num_jobs_per_machine + 1))
fi

split -l $num_jobs_per_machine $WORKDIR/all_shuffle_commands.sh $WORKDIR/shuffler_

shuffler_commands=$(ls $WORKDIR/shuffler_a*)
shuffler_services=()

index=0

echo "Launching shuffle scripts"

for s in $shuffler_commands; do
	machine=${machine_list[$index]}
	shuffler_service_name="shuffler_${index}-${RANDOM}"
	index=$((index + 1))
	shuffler_services+=("$shuffler_service_name")
	docker service create \
		-t \
		--name $shuffler_service_name \
		--mount $(get_service_mount_pattern $MOUNT_PATTERN) \
		--constraint "node.Hostname==$machine" \
		--restart-condition none \
		--entrypoint /bin/bash \
		$IMAGE_NAME \
		$(get_dockerized_path $s)
done

# Now wait for each service
for i in `seq 0 $((NUM_MACHINES - 1))`; do
	wait_for_service ${shuffler_services[$i]}
done

# Combine all partitions into the output files
echo "Combining shuffler results"
mkdir -p $WORKDIR/results
input_pattern_list=$(get_dockerized_path "$WORKDIR/shuffled/shuffled_partition-?????-*")
output_pattern_prefix=$(get_dockerized_path "$WORKDIR/results/combined")
output_dataset_config_pbtxt=$(get_dockerized_path "$WORKDIR/results/combined_pbtxt.txt")

printf "#!/bin/bash\n\n" > $WORKDIR/combiner_script.sh
printf "set -e\n\n" >> $WORKDIR/combiner_script.sh
printf "set -o pipefail\n\n" >> $WORKDIR/combiner_script.sh

# docker run --rm -t -v $MOUNT_PATTERN --entrypoint python $IMAGE_NAME \
printf "python /opt/tools/combine_tfrecords_beam.py \\
		--input_pattern_list=\"$input_pattern_list\" \\
		--output_pattern_prefix=\"$output_pattern_prefix\" \\
		--output_dataset_config_pbtxt=\"$output_dataset_config_pbtxt\" \\
		--output_dataset_name=\"results\" \\
		--direct_num_workers=$NUM_THREADS \\
		--direct_running_mode=\"multi_processing\" \\
		--streaming >& $(get_dockerized_path $WORKDIR)/combining.log" >> $WORKDIR/combiner_script.sh

combiner_name="combiner-${RANDOM}"

docker service create \
	-t \
	--name $combiner_name \
	--mount $(get_service_mount_pattern $MOUNT_PATTERN) \
	--constraint "node.Hostname==$machine" \
	--restart-condition none \
	--entrypoint /bin/bash \
	$IMAGE_NAME \
	$(get_dockerized_path $WORKDIR/combiner_script.sh)

wait_for_service $combiner_name
