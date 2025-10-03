for i in {1..50} ; do
   go test -run TestMySnapshotAllCrash2D
   rm *.log
done