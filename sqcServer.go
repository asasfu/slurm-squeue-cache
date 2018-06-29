package main

import (
  "fmt"
  "os"
  "os/exec"
  "bufio"
  "strings"
  "time"
  "net"
  "log"
)

func fetchInput(jobs *[]string, sqcmd string)([]string){
  cmd := exec.Command("bash","-c",sqcmd)
  o,err := cmd.Output()
  if err!=nil {
    fmt.Fprintf(os.Stderr,"fetchInput: %s\n", err.Error())
    return *jobs
  }

  var newjobs []string

  scanner := bufio.NewScanner(strings.NewReader(string(o)))
  for scanner.Scan(){
    newjobs = append(newjobs,scanner.Text()+"\n")
  }
  return newjobs
}

func listenRequest(jobs *[]string, port string){
  l,err := net.Listen("tcp",port)
  if err != nil { log.Fatal(err) }
  defer l.Close()

  for {
    conn,err := l.Accept()
    if err != nil { fmt.Fprintf(os.Stderr, "error: %s\n", err.Error()) }

    go func(c net.Conn){
      defer c.Close()
      c.SetReadDeadline(time.Now().Add(5*time.Second))
      for _,v := range *jobs {
        c.Write([]byte(v))
      }
    }(conn)
  }
}

func main(){
  var jobs []string
  sf := "\"'%A<|>%B<|>%C<|>%D<|>%E<|>%F<|>%G<|>%H<|>%I<|>%J<|>%K<|>%L<|>%M<|>%N<|>%O<|>%P<|>%Q<|>%R<|>%S<|>%T<|>%U<|>%V<|>%W<|>%X<|>%Y<|>%Z<|>%a<|>%b<|>%c<|>%d<|>%e<|>%f<|>%g<|>%h<|>%i<|>%j<|>%k<|>%l<|>%m<|>%n<|>%o<|>%p<|>%q<|>%r<|>%s<|>%t<|>%u<|>%v<|>%w<|>%x<|>%y<|>%z'\""

  var steps []string

  go listenRequest(&jobs,":6600")
  go listenRequest(&steps,":6601")

  for {
    jobs = fetchInput(&jobs,"squeue --format="+sf)
    time.Sleep(30*time.Second)
    steps = fetchInput(&steps,"squeue -s --format=\"'%A<|>%M<|>%N<|>%P<|>%S<|>%U<|>%b<|>%i<|>%j<|>%l<|>%u'\"")
    time.Sleep(30*time.Second)
  }
}
