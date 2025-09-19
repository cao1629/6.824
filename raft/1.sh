for i in {1..5} ; do
   go test -run TestSnapshotBasic2D
   rm *.log
done