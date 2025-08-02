package raft

import (
    "fmt"
    "log"
    "os"
    "strings"
    "sync/atomic"
)

var (
    logger = log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)
    rpcId  atomic.Uint32
)

func logRpc(caller int, callee int, isCaller bool, reply bool, rpcName string, term int, rpcId uint32, detail map[string]interface{}) {
    var b strings.Builder
    var server int
    if isCaller {
        server = caller
    } else {
        server = callee
    }
    b.WriteString(fmt.Sprintf("[%d %02d] ", server, term))
    if isCaller {
        b.WriteString(fmt.Sprintf("[%-15s %04d *%d -> %d] ", rpcName, rpcId, caller, callee))
    } else {
        b.WriteString(fmt.Sprintf("[%-15s %04d %d -> *%d] ", rpcName, rpcId, caller, callee))
    }

    if isCaller {
        if !reply {
            b.WriteString(fmt.Sprintf("%-12s", "RPC_ARGS: "))
        } else {
            b.WriteString(fmt.Sprintf("%-12s", "RPC_REPLY: "))
        }
    } else {
        b.WriteString(fmt.Sprintf("%-12s", "LOCAL: "))
    }

    for k, v := range detail {
        b.WriteString(fmt.Sprintf("%s{%v} ", k, v))
    }
    logger.Println(b.String())
}
