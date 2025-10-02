for i in {1..20} ; do
   go test -run TestSnapshotInstallUnreliable2D
   rm *.log
done