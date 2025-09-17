for i in {1..20} ; do
   go test -run TestSnapshotBasic2D
   rm *.log
done