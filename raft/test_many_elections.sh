for i in {1..20}; do
    go test -run TestManyElections2A > ./log/TestManyElections_log_${i} 
done
