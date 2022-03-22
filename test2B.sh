clearFunc() {
	DES_FOLDER=/tmp
	for FOLDER in $(ls $DES_FOLDER); do
		#  截取test
		test=$(expr substr $FOLDER 1 4)
		if [ "$test" = "test" ]; then
			$(rm -fr $DES_FOLDER/$FOLDER)
		fi
	done
}
echo -e "\n" > result
for ((i = 1; i <= 100; i++)); do
	echo "round $i"
	check_results=$(make project3b)
	# check_results=$( go test -v -run TestConfChangeRecoverManyClients3B ./kv/test_raftstore )
	# check_results=$( go test -v ./scheduler/server -check.f  TestRegionNotUpdate3C )     
	$(go clean -testcache)
	clearFunc
	if [[ $check_results =~ "FAIL" ]]; then
		echo "$check_results" > ./test3b/out5-"$i".log
		echo "fail->" >> result
		clearFunc
		# break
	fi
	echo "$i" >> result
	# echo "$check_results" > ./test3b/out-"$i".log

done
